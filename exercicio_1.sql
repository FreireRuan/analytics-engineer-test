-- ============================================================================
-- EXERCÍCIO 1 - SQL
-- ============================================================================

-- ============================================================================
-- PERGUNTA 1: Quais são os 50 maiores produtores em faturamento ($) de 2021?
-- ============================================================================
-- Objetivo: Somar o faturamento de cada produtor em 2021 e retornar os 50 maiores

with
    revenue as (
        select
            p.producer_id,
            sum(pi.purchase_value) total_revenue
        from 
            purchase p
            left join 
                product_item pi
                on 
                    p.prod_item_id = pi.prod_item_id
        where 
            p.release_date between '2021-01-01' and '2021-12-31'
        group by
            p.producer_id
    ),
    ranked as (
        select
            row_number() over(order by r.total_revenue desc) rw,
            r.producer_id,
            r.total_revenue
        from 
            revenue r
    )
    select
        producer_id,
        total_revenue
    from 
        ranked
    where 
        rw <= 50
;

-- ============================================================================
-- PERGUNTA 2: Quais são os 2 produtos que mais faturaram ($) de cada produtor?
-- ============================================================================
-- Objetivo: Somar o faturamento de cada produto para cada produtor e retornar os 2 maiores produtos por produtor

with 
    revenue_per_product as (
        select
            p.producer_id,
            pi.product_id,
            sum(pi.purchase_value) total_revenue
        from 
            purchase p
        left join 
            product_item pi
            on 
                p.prod_item_id = pi.prod_item_id
        group by 
            p.producer_id,
            pi.product_id
    ),
    ranked AS (
        select
            row_number() over(
                partition by r.producer_id
                order by r.total_revenue desc
            ) rw,
            r.producer_id,
            r.product_id,
            r.total_revenue
        from 
            revenue_per_product r
    )
select
    r.producer_id,
    r.product_id,
    r.total_revenue
from 
    ranked r
where
    rw <= 2
order by 
    1, 3 desc;