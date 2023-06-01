import dataclasses
from datetime import datetime, date
from textwrap import dedent


META_ALT_NAME = 'alternate_name'
META_SQL_PRE_INIT = 'pre_init'
META_SQL_KEY = 'sql_key'
META_SQL_NAME = 'sql_name'
META_SQL_TYPE = 'sql_type'
META_SQL_IGNORE = 'sql_ignore'
META_SQL_FOREIGN_KEY = 'sql_fk'


@dataclasses.dataclass
class SqlTable:

    @classmethod
    def field_names(cls):
        return [f.name for f in dataclasses.fields(cls)]

    @classmethod
    def field_sql_names(cls):
        return {
            f.metadata.get(META_SQL_NAME, f.name): f
            for f in dataclasses.fields(cls)
            if not f.metadata.get(META_SQL_IGNORE)
        }

    @classmethod
    def sql_table(cls, class_name=None):
        # Convert camel case to lower_with_underscores
        return ''.join(
            '_' + x.lower() if x.isupper() else x
            for x in cls.__name__
        ).strip('_')

    @classmethod
    def from_dict(cls, vals):
        cleaned = {}

        # Create a key translator for vals to fields
        keys_lower = {k.replace(' ', '').lower(): k for k in vals.keys()}

        for f in dataclasses.fields(cls):
            # Try to get the alternative name first
            val_key = f.metadata.get(META_ALT_NAME, keys_lower.get(f.name.lower()))
            if val_key:
                try:
                    if f.metadata.get(META_SQL_PRE_INIT):
                        cleaned[f.name] = f.metadata[META_SQL_PRE_INIT](vals[val_key])
                    elif vals[val_key] is not None and f.type != type(vals[val_key]):
                        cleaned[f.name] = f.type(vals[val_key])
                    else:
                        cleaned[f.name] = vals[val_key]
                except (TypeError, ValueError) as e:
                    raise type(e)(f'{e} at "{f.name}" value "{vals[val_key]}"')
                except KeyError as e:
                    raise type(e)(f'key "{val_key}" not found for field "{f.name}", all keys: {",".join(vals.keys())}')

        return cls(**cleaned)

    @classmethod
    def sql_create(cls):
        name = cls.sql_table()

        keys = []
        fields = []
        for field_name, field in cls.field_sql_names().items():

            if field.metadata.get(META_SQL_IGNORE):
                continue

            if field.metadata.get(META_SQL_KEY):
                keys.append(field_name)

            field_type = 'VARCHAR(255)'

            if field.metadata.get(META_SQL_NAME):
                field_name = field.metadata[META_SQL_NAME]

            if field.metadata.get(META_SQL_TYPE):
                field_type = field.metadata[META_SQL_TYPE]
            elif field.type == str:
                field_type = 'VARCHAR(255)'
            elif field.type == int:
                field_type = 'INT'
            elif field.type == float:
                field_type = 'FLOAT(20, 4)'
            elif field.type == datetime:
                field_type = 'DATETIME'
            elif field.type == date:
                field_type = 'DATE'
            elif field.type == bool:
                field_type = 'BOOLEAN DEFAULT FALSE'

            fields.append(f'{field_name} {field_type},')

        fields = f'\n{"":12s}'.join(fields)
        unique = f'UNIQUE({", ".join(keys)}),' if keys else ''
        index = ', '.join(['id'] + keys)

        sql = f'''
        CREATE TABLE IF NOT EXISTS `{name}` (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            created DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Time stamp of record creation',

            {fields}

            {unique}
            PRIMARY KEY (id),
            KEY {name}_idx ({index})
        ) ENGINE = INNODB
        '''

        return dedent(sql).strip()

    @classmethod
    def sql_insert(cls):
        fields = cls.field_sql_names()

        on_update = [
            f'{s}={s}'
            for s, f in fields.items()
            if f.metadata.get(META_SQL_KEY)
        ]

        update = f'ON DUPLICATE KEY UPDATE {", ".join(on_update)}' if on_update else ''

        insert_dict = {}
        for k, field in fields.items():
            val = f'%({field.name})s'
            if field.metadata.get(META_SQL_FOREIGN_KEY) is not None:
                fk_table, fk_field = field.metadata[META_SQL_FOREIGN_KEY]
                val = f'(SELECT id from {fk_table} where {fk_field}={val} LIMIT 1)'

            insert_dict[k] = val

        sql = f'''
        INSERT INTO `{cls.sql_table()}` (
            {", ".join(insert_dict.keys())}
        )
        VALUES (
            {", ".join(insert_dict.values())}
        )
        {update}
        '''

        return dedent(sql).strip()

    @classmethod
    def sql_update(cls):
        fields = cls.field_sql_names()

        where = [
            f'{s}=%({f.name})s'
            for s, f in fields.items()
            if f.metadata.get(META_SQL_KEY)
        ]

        update_list = []
        for k, field in fields.items():
            val = f'%({field.name})s'
            if field.metadata.get(META_SQL_FOREIGN_KEY) is not None:
                continue

            update_list.append(f'{k}={val}')

        sql = f'''
        UPDATE `{cls.sql_table()}`
        SET
            {", ".join(update_list)}
        WHERE
            {" AND ".join(where)}
        '''

        return dedent(sql).strip()


    @classmethod
    def sql_drop(cls):
        return f'DROP TABLE IF EXISTS `{cls.sql_table()}`'

    @classmethod
    def sql_delete(cls):
        return f'DELETE FROM `{cls.sql_table()}`'

    @classmethod
    def sql_prune(cls, ignore_num_days, date_column='created'):
        sql = f'''
        DELETE FROM `{cls.sql_table()}`
        WHERE id IN (SELECT id FROM(
            SELECT  id
            FROM    {cls.sql_table()}
            WHERE   {date_column} < CURDATE() - INTERVAL {ignore_num_days} DAY
            AND     DATE({date_column}) NOT IN (
                    SELECT DATE(MIN({date_column})) d
                    FROM {cls.sql_table()}
                    GROUP BY DATE_FORMAT({date_column}, '%Y-%m')
                )
        ) AS x)
        '''

        return dedent(sql).strip()
