Date_dim = [['d_date_sk', 'integer'],
            ['d_date_id', 'char(16)'],
            ['d_date', 'date'],
            ['d_month_seq', 'integer'],
            ['d_week_seq', 'integer'],
            ['d_quarter_seq', 'integer'],
            ['d_year', 'integer'],
            ['d_dow', 'integer'],
            ['d_moy', 'integer'],
            ['d_dom', 'integer'],
            ['d_qoy', 'integer'],
            ['d_fy_year', 'integer'],
            ['d_fy_quarter_seq', 'integer'],
            ['d_fy_week_seq', 'integer'],
            ['d_day_name', 'char(9)'],
            ['d_quarter_name', 'char(6)'],
            ['d_holiday', 'char(1)'],
            ['d_weekend', 'char(1)'],
            ['d_following_holiday', 'char(1)'],
            ['d_first_dom', 'integer'],
            ['d_last_dom', 'integer'],
            ['d_same_day_ly', 'integer'],
            ['d_same_day_lq', 'integer'],
            ['d_current_day', 'char(1)'],
            ['d_current_week', 'char(1)'],
            ['d_current_month', 'char(1)'],
            ['d_current_quarter', 'char(1)'],
            ['d_current_year', 'char(1)']]
Catalog_sales = [
    ['cs_sold_date_sk',           'integer'],
    ['cs_sold_time_sk',           'integer'],
    ['cs_ship_date_sk',           'integer'],
    ['cs_bill_customer_sk',       'integer'],
    ['cs_bill_cdemo_sk',          'integer'],
    ['cs_bill_hdemo_sk',          'integer'],
    ['cs_bill_addr_sk',           'integer'],
    ['cs_ship_customer_sk',       'integer'],
    ['cs_ship_cdemo_sk',          'integer'],
    ['cs_ship_hdemo_sk',          'integer'],
    ['cs_ship_addr_sk',           'integer'],
    ['cs_call_center_sk',         'integer'],
    ['cs_catalog_page_sk',        'integer'],
    ['cs_ship_mode_sk',           'integer'],
    ['cs_warehouse_sk',           'integer'],
    ['cs_item_sk',                'integer'],
    ['cs_promo_sk',               'integer'],
    ['cs_order_number',           'integer'],
    ['cs_quantity',               'integer'],
    ['cs_wholesale_cost',         'decimal(7,2)'],
    ['cs_list_price',             'decimal(7,2)'],
    ['cs_sales_price',            'decimal(7,2)'],
    ['cs_ext_discount_amt',       'decimal(7,2)'],
    ['cs_ext_sales_price',        'decimal(7,2)'],
    ['cs_ext_wholesale_cost',     'decimal(7,2)'],
    ['cs_ext_list_price',         'decimal(7,2)'],
    ['cs_ext_tax',                'decimal(7,2)'],
    ['cs_coupon_amt',             'decimal(7,2)'],
    ['cs_ext_ship_cost',          'decimal(7,2)'],
    ['cs_net_paid',               'decimal(7,2)'],
    ['cs_net_paid_inc_tax',       'decimal(7,2)'],
    ['cs_net_paid_inc_ship',      'decimal(7,2)'],
    ['cs_net_paid_inc_ship_tax',  'decimal(7,2)'],
    ['cs_net_profit',             'decimal(7,2)']
]

Catalog_returns = [
    ['cr_order_number'           ,'integer'     ]
]
Customer_address = [
    ['ca_address_sk'             ,'integer'     ],
    ['ca_address_id'             ,'char(16)'    ],
    ['ca_street_number'          ,'char(10)'    ],
    ['ca_street_name'            ,'varchar(60)' ],
    ['ca_street_type'            ,'char(15)'    ],
    ['ca_suite_number'           ,'char(10)'    ],
    ['ca_city'                   ,'varchar(60)' ],
    ['ca_county'                 ,'varchar(30)' ],
    ['ca_state'                  ,'char(2)'     ],
    ['ca_zip'                    ,'char(10)'    ],
    ['ca_country'                ,'varchar(20)' ],
    ['ca_gmt_offset'             ,'decimal(5,2)'],
    ['ca_location_type'          ,'char(20)'    ]
]

Call_center = [
    ['cc_call_center_sk'         ,'integer'     ],
    ['cc_call_center_id'         ,'char(16)'    ],
    ['cc_rec_start_date'         ,'date'        ],
    ['cc_rec_end_date'           ,'date'        ],
    ['cc_closed_date_sk'         ,'integer'     ],
    ['cc_open_date_sk'           ,'integer'     ],
    ['cc_name'                   ,'varchar(50)' ],
    ['cc_class'                  ,'varchar(50)' ],
    ['cc_employees'              ,'integer'     ],
    ['cc_sq_ft'                  ,'integer'     ],
    ['cc_hours'                  ,'char(20)'    ],
    ['cc_manager'                ,'varchar(40)' ],
    ['cc_mkt_id'                 ,'integer'     ],
    ['cc_mkt_class'              ,'char(50)'    ],
    ['cc_mkt_desc'               ,'varchar(100)'],
    ['cc_market_manager'         ,'varchar(40)' ],
    ['cc_division'               ,'integer'     ],
    ['cc_division_name'          ,'varchar(50)' ],
    ['cc_company'                ,'integer'     ],
    ['cc_company_name'           ,'char(50)'    ],
    ['cc_street_number'          ,'char(10)'    ],
    ['cc_street_name'            ,'varchar(60)' ],
    ['cc_street_type'            ,'char(15)'    ],
    ['cc_suite_number'           ,'char(10)'    ],
    ['cc_city'                   ,'varchar(60)' ],
    ['cc_county'                 ,'varchar(30)' ],
    ['cc_state'                  ,'char(2)'     ],
    ['cc_zip'                    ,'char(10)'    ],
    ['cc_country'                ,'varchar(20)' ],
    ['cc_gmt_offset'             ,'decimal(5,2)'],
    ['cc_tax_percentage'         ,'decimal(5,2)']
]
all_schemas = {'date_dim': Date_dim, 'catalog_sales': Catalog_sales,
               'catalog_returns': Catalog_returns, 'customer_address': Customer_address,
               'call_center': Call_center}


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