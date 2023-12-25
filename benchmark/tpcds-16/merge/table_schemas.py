
Catalog_sales = [
    ['cs_order_number',           'integer'],
    ['cs_ext_ship_cost',          'decimal(7,2)'],
    ['cs_net_profit',             'decimal(7,2)']
]

all_schemas = {'catalog_sales': Catalog_sales}


def schemas():
    for schema in all_schemas:
        table_schema = all_schemas[schema]
        for key, field in enumerate(table_schema):
            if 'char' in field[1]:
                table_schema[key][1] = 'str'
            elif field[1] == 'integer' or 'decimal' in field[1]:
                table_schema[key][1] = 'int'
            elif field[1] == 'identifier':
                table_schema[key][1] = 'str'
    return all_schemas

if __name__ == '__main__':
    pass