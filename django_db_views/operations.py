from functools import partial
from typing import cast

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations import operations
from django.db.migrations.state import ModelState
from django.db import models

from django_db_views.context_manager import VIEW_MIGRATION_CONTEXT
from django_db_views.db_view import DBMaterializedView, DBView
from django_db_views.migration_functions import (
    ForwardMaterializedViewMigration,
    ForwardViewMigration,
)


def get_table_engine_name_hash(table_name, engine):
    return f"{table_name}_{engine}".lower()


class DBViewModelState(ModelState):
    def __init__(
        self,
        *args,
        # Not required cus migrate also load state using clone method that do not provide
        # required by us fields.
        view_engine: str = None,
        view_definition: str = None,
        table_name: str = None,
        base_class=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if VIEW_MIGRATION_CONTEXT["is_view_migration"]:
            self.view_engine = view_engine
            self.view_definition = view_definition
            self.base_class = base_class
            self.table_name = table_name


class ViewRunPython(operations.RunPython):
    reduces_to_sql = True

    def state_forwards(self, app_label, state):
        if VIEW_MIGRATION_CONTEXT["is_view_migration"]:
            if isinstance(self.code, ForwardMaterializedViewMigration):
                model = DBMaterializedView
            elif isinstance(self.code, ForwardViewMigration):
                model = DBView
            else:
                raise NotImplementedError
            state.add_model(
                DBViewModelState(
                    app_label,
                    # Hash table_name_engine_name to add state model per migration, which are
                    # added per engine.
                    get_table_engine_name_hash(
                        self.code.table_name, self.code.view_engine
                    ),
                    list(),
                    dict(),
                    # we do not use django bases (they initialize model using that, and broke
                    # ViewRegistry),
                    # instead of that we pass bass class in separate argument.
                    tuple(),
                    list(),
                    view_engine=self.code.view_engine,
                    view_definition=self.code.view_definition,
                    base_class=model,
                    table_name=self.code.table_name,
                )
            )

    def describe(self):
        return "View migration operation"


class ViewDropRunPython(operations.RunPython):
    def state_forwards(self, app_label, state):
        if VIEW_MIGRATION_CONTEXT["is_view_migration"]:
            state.remove_model(
                app_label,
                get_table_engine_name_hash(self.code.table_name, self.code.view_engine),
            )


def add_field_comment(self: BaseDatabaseSchemaEditor, model, field):
    """
    Create a field on a model. Usually involves adding a column, but may
    involve adding a table instead (for M2M fields).
    """

    db_params = field.db_parameters(connection=self.connection)
    # Add field comment, if required.
    if (
        field.db_comment
        and self.connection.features.supports_comments
        and not self.connection.features.supports_comments_inline
    ):
        field_type = db_params["type"]
        self.execute(
            *self._alter_column_comment_sql(model, field, field_type, field.db_comment)
        )
    # Reset connection if required
    if self.connection.features.connection_persists_old_columns:
        self.connection.close()


class AddFieldComment(operations.AddField):

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        to_model = cast(
            type[models.Model], to_state.apps.get_model(app_label, self.model_name)
        )
        from_model = cast(
            type[models.Model], from_state.apps.get_model(app_label, self.model_name)
        )
        field = to_model._meta.get_field(self.name)
        schema_editor.add_field = partial(add_field_comment, schema_editor)
        schema_editor.add_field(from_model, field)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        from_model = cast(
            type[models.Model], from_state.apps.get_model(app_label, self.model_name)
        )
        schema_editor.remove_field(from_model, from_model._meta.get_field(self.name))

    def describe(self):
        return "Add COMMENT to field %s on %s" % (self.name, self.model_name)

    @property
    def migration_name_fragment(self):
        return "comment_%s_%s" % (self.model_name_lower, self.name_lower)

    def reduce(self, operation, app_label):
        if isinstance(operation, operations.fields.FieldOperation) and self.is_same_field_operation(
            operation
        ):
            if isinstance(operation, AlterFieldComment):
                return [
                    AddFieldComment(
                        model_name=self.model_name,
                        name=operation.name,
                        field=operation.field,
                    ),
                ]
        return super().reduce(operation, app_label)


def _alter_field_comment(
    self,
    model,
    old_field,
    new_field,
    _old_type,
    _new_type,
    _old_db_params,
    _new_db_params,
    _strict=False,
):
    actions = []
    # Comment change?
    if self.connection.features.supports_comments and not new_field.many_to_many:
        if old_field.db_comment != new_field.db_comment:
            # PostgreSQL and Oracle can't execute 'ALTER COLUMN ...' and
            # 'COMMENT ON ...' at the same time.
            sql, params = self._alter_column_comment_sql(
                model, new_field, _new_type, new_field.db_comment
            )
            if sql:
                actions.append((sql, params))
    if actions:
        for sql, params in actions:
            self.execute(sql, params)

    if self.connection.features.connection_persists_old_columns:
        self.connection.close()


class AlterFieldComment(operations.AlterField):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        to_model = cast(
            type[models.Model], to_state.apps.get_model(app_label, self.model_name)
        )
        from_model = cast(
            type[models.Model], from_state.apps.get_model(app_label, self.model_name)
        )
        from_field = from_model._meta.get_field(self.name)
        to_field = to_model._meta.get_field(self.name)
        schema_editor._alter_field = partial(_alter_field_comment, schema_editor)
        schema_editor.alter_field(from_model, from_field, to_field)

    def describe(self):
        return "Alter comment of field %s on %s" % (self.name, self.model_name)

    @property
    def migration_name_fragment(self):
        return "alter_comment_%s_%s" % (self.model_name_lower, self.name_lower)