package sql

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type Driver[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]] struct {
	// URL at which documentation for the driver may be found.
	DocumentationURL string
	// StartTunnel starts up an SSH tunnel if one is configured prior to running
	// any operations that require connectivity to the database.
	StartTunnel func(ctx context.Context, cfg EC) error
	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
	NewEndpoint func(_ context.Context, cfg EC, featureFlags map[string]bool) (*Endpoint[EC], error)
	// PreReqs performs verification checks that the provided configuration can
	// be used to interact with the endpoint to the degree required by the
	// connector, to as much of an extent as possible. The returned PrereqErr
	// can include multiple separate errors if it possible to determine that
	// there is more than one issue that needs corrected.
	PreReqs func(ctx context.Context, cfg EC) *cerrors.PrereqErr
}

var _ boilerplate.Connector = &Driver[boilerplate.EndpointConfiger, Resource]{}

// docsUrlFromEnv looks for an environment variable set as DOCS_URL to use for the spec response
// documentation URL. It uses that instead of the default documentation URL from the connector if
// found.
func docsUrlFromEnv(providedURL string) string {
	fromEnv := os.Getenv("DOCS_URL")
	if fromEnv != "" {
		return fromEnv
	}

	return providedURL
}

func (d *Driver[EC, RC]) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpoint, resource []byte

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = schemagen.GenerateSchema("SQL Connection", new(EC)).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	} else if resource, err = schemagen.GenerateSchema("SQL Table", new(RC)).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpoint),
		ResourceConfigSchemaJson: json.RawMessage(resource),
		DocumentationUrl:         docsUrlFromEnv(d.DocumentationURL),
	}, nil
}

func (d *Driver[EC, RC]) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, d.newMaterialization)
}

func (d *Driver[EC, RC]) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, d.newMaterialization)
}

func (d *Driver[EC, RC]) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, d.newMaterialization)
}

type sqlMaterialization[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]] struct {
	materializationName string
	featureFlags        map[string]bool
	driver              *Driver[EC, RC]
	endpoint            *Endpoint[EC]
	client              Client
}

func (d *Driver[EC, RC]) newMaterialization(ctx context.Context, materializationName string, cfg EC, featureFlags map[string]bool) (boilerplate.Materializer[EC, FieldConfig, RC, MappedType], error) {
	if err := d.StartTunnel(ctx, cfg); err != nil {
		return nil, fmt.Errorf("starting network tunnel: %w", err)
	}

	endpoint, err := d.NewEndpoint(ctx, cfg, featureFlags)
	if err != nil {
		return nil, fmt.Errorf("creating endpoint: %w", err)
	}

	client, err := endpoint.NewClient(ctx, materializationName, endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &sqlMaterialization[EC, RC]{
		materializationName: materializationName,
		featureFlags:        featureFlags,
		driver:              d,
		endpoint:            endpoint,
		client:              client,
	}, nil
}

var _ boilerplate.Materializer[
	boilerplate.EndpointConfiger,
	FieldConfig,
	Resource,
	MappedType,
] = &sqlMaterialization[boilerplate.EndpointConfiger, Resource]{}

func (s *sqlMaterialization[EC, RC]) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return s.driver.PreReqs(ctx, s.endpoint.Config)
}

func (s *sqlMaterialization[EC, RC]) Config() boilerplate.MaterializeCfg {
	_, doCreateSchemas := s.client.(SchemaManager)

	return boilerplate.MaterializeCfg{
		Locate:                   ToLocatePathFn(s.endpoint.Dialect.TableLocator),
		TranslateNamespace:       s.endpoint.SchemaLocator,
		TranslateField:           s.endpoint.ColumnLocator,
		MaxFieldLength:           s.endpoint.Dialect.MaxColumnCharLength,
		CaseInsensitiveFields:    s.endpoint.Dialect.CaseInsensitiveColumns,
		CaseInsensitiveResources: s.endpoint.Dialect.CaseInsensitiveResources,
		ConcurrentApply:          s.endpoint.ConcurrentApply,
		NoCreateNamespaces:       !doCreateSchemas,
		SerPolicy:                s.endpoint.SerPolicy,
		MaterializeOptions:       s.endpoint.Options,
	}
}

func (s *sqlMaterialization[EC, RC]) CreateNamespace(ctx context.Context, ns string) (string, error) {
	desc, err := s.client.(SchemaManager).CreateSchema(ctx, ns)
	if err != nil {
		return "", fmt.Errorf("creating schema: %w", err)
	}

	return desc, nil
}

func (s *sqlMaterialization[EC, RC]) CreateResource(ctx context.Context, binding boilerplate.MappedBinding[EC, RC, MappedType]) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(s.endpoint, s.materializationName, binding)
	if err != nil {
		return "", nil, err
	}

	if p, ok := s.client.(TablePreparer); ok {
		if err := p.PrepareTable(&table); err != nil {
			return "", nil, fmt.Errorf("preparing table: %w", err)
		}
	}

	createStatement, err := RenderTableTemplate(table, s.endpoint.CreateTableTemplate)
	if err != nil {
		return "", nil, err
	}

	return createStatement, func(ctx context.Context) error {
		if err := s.client.CreateTable(ctx, TableCreate{
			Table:          table,
			TableCreateSql: createStatement,
			Resource:       binding.Config,
		}); err != nil {
			log.WithFields(log.Fields{
				"table":          table.Identifier,
				"tableCreateSql": createStatement,
			}).Error("table creation failed")
			return fmt.Errorf("failed to create table %q: %w", table.Identifier, err)
		}

		return nil
	}, nil
}

func (s *sqlMaterialization[EC, RC]) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return s.client.DeleteTable(ctx, path)
}

func (s *sqlMaterialization[EC, RC]) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return s.client.TruncateTable(ctx, path)
}

func (s *sqlMaterialization[EC, RC]) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return s.client.MustRecreateResource(req, lastBinding, newBinding)
}

func (s *sqlMaterialization[EC, RC]) MapType(p boilerplate.Projection, fc FieldConfig) (MappedType, boilerplate.ElementConverter) {
	pp := buildProjection(&p.Projection)

	m := s.endpoint.Dialect.MapType(&pp, fc)
	m.MigratableTypes = s.endpoint.Dialect.MigratableTypes

	return m, boilerplate.ElementConverter(m.Converter)
}

func (s *sqlMaterialization[EC, RC]) NewConstraint(p pf.Projection, deltaUpdates bool, fc FieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := m.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "All Locations that are part of the collections key are recommended"
	case p.IsRootDocumentProjection() && s.endpoint.NoFlowDocument:
		// When flow_document is disabled, root document projection becomes optional
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Root document projection is optional when flow_document is disabled"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case !deltaUpdates && s.endpoint.NoFlowDocument && strings.Count(p.Ptr, "/") == 1 && p.Inference.Exists == pf.Inference_MUST:
		// When flow_document is disabled, all root-level properties become LOCATION_REQUIRED
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Required root-level properties must be present when flow_document is disabled"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (s *sqlMaterialization[EC, RC]) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, paths [][]string) error {
	if s.endpoint.MetaCheckpoints != nil {
		paths = append(paths, s.endpoint.MetaCheckpoints.Path)
	}

	if schemaLister, ok := s.client.(SchemaManager); ok {
		schemas, err := schemaLister.ListSchemas(ctx)
		if err != nil {
			return fmt.Errorf("listing schemas: %w", err)
		}
		for _, schema := range schemas {
			is.PushNamespace(schema)
		}
	}

	return s.client.PopulateInfoSchema(ctx, is, paths)
}

func (s *sqlMaterialization[EC, RC]) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	// Create the checkpoints table if it doesn't already exist & this endpoint
	// needs a checkpoints table.
	var createStatement string
	if s.endpoint.MetaCheckpoints != nil && is.GetResource(s.endpoint.MetaCheckpoints.Path) == nil {
		if resolved, err := ResolveTable(*s.endpoint.MetaCheckpoints, s.endpoint.Dialect); err != nil {
			return "", err
		} else if createStatement, err = RenderTableTemplate(resolved, s.endpoint.CreateTableTemplate); err != nil {
			return "", err
		} else if err := s.client.CreateTable(ctx, TableCreate{
			Table:          resolved,
			TableCreateSql: createStatement,
		}); err != nil {
			return "", fmt.Errorf("creating checkpoints table: %w", err)
		}
	}

	return createStatement, nil
}

func (s *sqlMaterialization[EC, RC]) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existingResource boilerplate.ExistingResource,
	bindingUpdate boilerplate.BindingUpdate[EC, RC, MappedType],
) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(s.endpoint, s.materializationName, bindingUpdate.Binding)
	if err != nil {
		return "", nil, err
	}

	if p, ok := s.client.(TablePreparer); ok {
		if err := p.PrepareTable(&table); err != nil {
			return "", nil, fmt.Errorf("preparing table: %w", err)
		}
	}

	getColumn := func(field string) (Column, error) {
		for _, c := range table.Columns() {
			if field == c.Field {
				return *c, nil
			}
		}
		return Column{}, fmt.Errorf("could not find column for field %q in table %s", field, table.Identifier)
	}

	alter := TableAlter{
		Table:        table,
		DropNotNulls: bindingUpdate.NewlyNullableFields,
	}

	for _, newProjection := range bindingUpdate.NewProjections {
		col, err := getColumn(newProjection.Field)
		if err != nil {
			return "", nil, err
		}

		if existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil {
			// At this stage we don't have the target MappedType anymore, but
			// it's okay because if we don't have the original column anymore
			// (hence the new projection), it means we have already created the
			// new column and set its value.
			alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, ColumnTypeMigration{
				Column:               col,
				ProgressColumnExists: true,
				OriginalColumnExists: false,
			})
			continue
		}
		alter.AddColumns = append(alter.AddColumns, col)
	}

	for _, migrate := range bindingUpdate.FieldsToMigrate {
		col, err := getColumn(migrate.To.Field)
		if err != nil {
			return "", nil, err
		}

		migrationSpec := s.endpoint.Dialect.MigratableTypes.FindMigrationSpec(migrate.From, migrate.To.Mapped)
		var m = ColumnTypeMigration{
			Column:               col,
			MigrationSpec:        *migrationSpec,
			OriginalColumnExists: true,
			ProgressColumnExists: existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil,
		}
		alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, m)
	}

	// If there is nothing to do, skip
	if len(alter.AddColumns) == 0 && len(alter.DropNotNulls) == 0 && len(alter.ColumnTypeChanges) == 0 {
		return "", nil, nil
	}

	return s.client.AlterTable(ctx, alter)
}

func (s *sqlMaterialization[EC, RC]) NewTransactor(
	ctx context.Context,
	open pm.Request_Open,
	is boilerplate.InfoSchema,
	bindings []boilerplate.MappedBinding[EC, RC, MappedType],
	be *m.BindingEvents,
) (m.Transactor, error) {
	tables := make([]Table, 0, len(bindings))
	for _, binding := range bindings {
		table, err := getTable(s.endpoint, s.materializationName, binding)
		if err != nil {
			return nil, fmt.Errorf("getting table for binding %d: %w", binding.Index, err)
		}
		table.StateKey = binding.StateKey
		tables = append(tables, table)
	}

	var fence = Fence{
		TablePath:       nil, // Set later iff endpoint.MetaCheckpoints != nil.
		Materialization: open.Materialization.Name,
		KeyBegin:        open.Range.KeyBegin,
		KeyEnd:          open.Range.KeyEnd,
		Fence:           0,
		Checkpoint:      nil, // Set later iff endpoint.MetaCheckpoints != nil.
	}

	if checkpointsShape := s.endpoint.MetaCheckpoints; checkpointsShape != nil {
		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*checkpointsShape, s.endpoint.Dialect)
		if err != nil {
			return nil, fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = checkpointsShape.Path
		fence.Checkpoint = pm.ExplicitZeroCheckpoint

		fence, err = s.client.InstallFence(ctx, metaCheckpoints, fence)
		if err != nil {
			return nil, fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	return s.endpoint.NewTransactor(ctx, s.materializationName, s.featureFlags, s.endpoint, fence, tables, open, &is, be)
}

func (s *sqlMaterialization[EC, RC]) FlushDDL(ctx context.Context) error {
	if flusher, ok := s.client.(boilerplate.DDLFlusher); ok {
		if err := flusher.FlushDDL(ctx); err != nil {
			return fmt.Errorf("flushing batched DDL: %w", err)
		}
	}

	return nil
}

func (s *sqlMaterialization[EC, RC]) ListTestTasks(ctx context.Context) ([]string, error) {
	return s.client.ListCheckpointsEntries(ctx)
}

func (s *sqlMaterialization[EC, RC]) CleanupTestTask(ctx context.Context, taskName string) error {
	return s.client.DeleteCheckpointsEntry(ctx, taskName)
}

func (s *sqlMaterialization[EC, RC]) SnapshotTestResource(ctx context.Context, path []string) (columnNames []string, rows [][]any, _ error) {
	return s.client.SnapshotTestTable(ctx, path)
}

func (s *sqlMaterialization[EC, RC]) Close(ctx context.Context) {
	s.client.Close()
}

func getTable[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](endpoint *Endpoint[EC], materializationName string, binding boilerplate.MappedBinding[EC, RC, MappedType]) (Table, error) {
	path, delta, err := binding.Config.Parameters()
	if err != nil {
		return Table{}, fmt.Errorf("getting parameters for binding %d: %w", binding.Index, err)
	}
	tableShape := BuildTableShape(materializationName, &binding.MaterializationSpec_Binding, binding.Index, path, delta)
	return ResolveTable(tableShape, endpoint.Dialect)
}

// TxDefuser.MaybeRollback calls Rollback on a Tx, unless Defuse has previously
// been called.
type TxDefuser struct {
	defused bool
	txn     *stdsql.Tx
}

func NewTxDefuser(txn *stdsql.Tx) *TxDefuser {
	return &TxDefuser{
		defused: false,
		txn:     txn,
	}
}

func (t *TxDefuser) MaybeRollback() {
	if t.defused {
		return
	}
	t.txn.Rollback()
}

func (t *TxDefuser) Defuse() {
	t.defused = true
}

// RowReader is an optional Client capability: enumerate the rows a materialized
// table holds, as one JSON object per row keyed by column name.
//
// Optional because it exists only for verification, and a client that cannot offer
// it should not be forced to. Most implementations are one line over StdReadRows.
type RowReader interface {
	ReadRows(ctx context.Context, path []string, out func(json.RawMessage) error) error
}

// ReadDestination implements boilerplate.DestinationReader for every SQL connector,
// by delegating to its Client's optional RowReader.
//
// Rows come back as flat column-name objects rather than as the collection documents
// that produced them. That is the honest shape: a materialized table *is* columns, a
// standard binding need not carry a root document at all (see the connectors'
// `no_flow_document` option), and reconstructing the original document would mean
// re-implementing the connector's projections in reverse. A harness comparing against
// a collection has to reckon with the mapping either way, so this does not hide it.
func (d *Driver[EC, RC]) ReadDestination(
	ctx context.Context,
	endpointConfig json.RawMessage,
	resourceConfig json.RawMessage,
	out func(json.RawMessage) error,
) error {
	var cfg EC
	if err := boilerplate.UnmarshalStrict(endpointConfig, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating endpoint config: %w", err)
	}

	var resource RC
	if err := boilerplate.UnmarshalStrict(resourceConfig, &resource); err != nil {
		return fmt.Errorf("parsing resource config: %w", err)
	}
	resource = resource.WithDefaults(cfg)
	if err := resource.Validate(); err != nil {
		return fmt.Errorf("validating resource config: %w", err)
	}
	path, _, err := resource.Parameters()
	if err != nil {
		return fmt.Errorf("resource parameters: %w", err)
	}

	if d.StartTunnel != nil {
		if err := d.StartTunnel(ctx, cfg); err != nil {
			return fmt.Errorf("starting tunnel: %w", err)
		}
	}

	// The materialization name only labels the connection, and a read belongs to no
	// materialization in particular.
	const readerName = "destination-read"

	endpoint, err := d.NewEndpoint(ctx, cfg, nil)
	if err != nil {
		return fmt.Errorf("creating endpoint: %w", err)
	}
	client, err := endpoint.NewClient(ctx, readerName, endpoint)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	defer client.Close()

	reader, ok := client.(RowReader)
	if !ok {
		return fmt.Errorf("this connector's client cannot read rows: it does not implement sql.RowReader")
	}
	return reader.ReadRows(ctx, path, out)
}

// StdReadRows enumerates a table's rows over a database/sql handle, emitting each as
// a JSON object keyed by column name. Values arrive as whatever the driver yields;
// []byte is treated as text, which is how JSON and string columns come back.
func StdReadRows(
	ctx context.Context,
	db *stdsql.DB,
	identifier string,
	out func(json.RawMessage) error,
) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", identifier))
	if err != nil {
		return fmt.Errorf("querying %s: %w", identifier, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("reading columns of %s: %w", identifier, err)
	}

	for rows.Next() {
		var values = make([]any, len(columns))
		var into = make([]any, len(columns))
		for i := range values {
			into[i] = &values[i]
		}
		if err := rows.Scan(into...); err != nil {
			return fmt.Errorf("scanning a row of %s: %w", identifier, err)
		}

		var doc = make(map[string]any, len(columns))
		for i, column := range columns {
			if raw, ok := values[i].([]byte); ok {
				doc[column] = string(raw)
			} else {
				doc[column] = values[i]
			}
		}
		encoded, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("encoding a row of %s: %w", identifier, err)
		}
		if err := out(encoded); err != nil {
			return err
		}
	}
	return rows.Err()
}
