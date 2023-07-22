def dwh_dim_users():
    query = """
    select	u.id as user_id,
            concat(u.first_name,' ',u.last_name) as name,
            u.email,
            u.gender,
            u.age,
            u.street_address,
            u.state,
            u.country,
            u.traffic_source
    from users as u
    order by id
    """
    return query

def dwh_dim_product():
    query = """
    select 	id as product_id,
            "name" as product_name,
            "cost",
            category,
            brand,
            department,
            retail_price
    from product p
    order by id
    """
    return query

def dwh_dim_distribution():
    query = """
    select 	id as distribution_centers_id,
		    "name" as distribution_centers_name
    from distribution_centers dc
    """
    return query


def dwh_dim_order_date():
    query = """
    select 	id ,
            order_id,
            to_date(created_at, 'YYYY-MM-DD') as created_at,
            to_date(returned_at, 'YYYY-MM-DD') as returned_at,
            to_date(shipped_at, 'YYYY-MM-DD') as shipped_at,
            to_date(delivered_at, 'YYYY-MM-DD') as delivered_at
    from order_item oi
    order by id
    """
    return query


def dwh_fact_order():
    query = """
    select	oi.order_id,
            oi.user_id,
            oi.product_id,
            p.distribution_center_id,
            oi.status,
            oi.sale_price as price
    from order_item oi
    inner join product p on p.id = oi.product_id
    order by order_id
    """
    return query

def dwh_fact_transaction_order():
    query = """
    select 	oi.order_id,
            oi.user_id,
            oi.status,
            count(oi.product_id) as quantity,
            sum(oi.sale_price) as total_price
    from order_item oi
    group by oi.user_id, oi.order_id, oi.status
    order by order_id
    """
    return query

def dm_day_transaction_s_created():
    query = """
    select 	ddo.created_at,
            count(ddo.created_at)
    from dim_datetime_order ddo
    where ddo.created_at notnull
    group by ddo.created_at
    order by ddo.created_at
    """
    return query

def dm_day_transaction_s_returned():
    query = """
    select 	ddo.returned_at,
            count(ddo.returned_at)
    from dim_datetime_order ddo
    where ddo.returned_at  notnull
    group by ddo.returned_at
    order by ddo.returned_at
    """
    return query

def dm_day_transaction_s_shipped():
    query = """
    select 	ddo.shipped_at,
            count(ddo.shipped_at)
    from dim_datetime_order ddo
    where ddo.shipped_at  notnull
    group by ddo.shipped_at
    order by ddo.shipped_at
    """
    return query

def dm_day_transaction_s_delivered():
    query = """
    select 	ddo.delivered_at ,
            count(ddo.delivered_at)
    from dim_datetime_order ddo
    where ddo.delivered_at  notnull
    group by ddo.delivered_at
    order by ddo.delivered_at
    """
    return query



def list_tables():
    tables = [("dim_user",dwh_dim_users()),
              ("dim_product",dwh_dim_product()),
              ("dim_distribution_center",dwh_dim_distribution()),
              ("dim_datetime_order",dwh_dim_order_date()),
              ("fact_order",dwh_fact_order()),
              ("fact_transaction_order",dwh_fact_transaction_order())]
    return tables

def list_tables_datamart():
    tables = [("dm_day_transaction_s_created",dm_day_transaction_s_created()),
              ("dm_day_transaction_s_shipped",dm_day_transaction_s_shipped()),
              ("dm_day_transaction_s_delivered",dm_day_transaction_s_delivered()),
              ("dm_day_transaction_s_returned",dm_day_transaction_s_returned())]
    return tables