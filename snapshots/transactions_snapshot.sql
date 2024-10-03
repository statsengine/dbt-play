{% snapshot transactions_snapshot %}

    {{
        config(
            target_schema='snapshots',
            strategy='check',
            unique_key='transaction_id',
            check_cols=['payment_status','payment_method']
        )
    }}

    select * from {{ source('ecommerce', 'ecommerce_transactions') }}

{% endsnapshot %}   