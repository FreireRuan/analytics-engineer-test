-- ============================================================================
-- EXERCÍCIO 2 - Consultas SQL para GMV
-- ============================================================================
-- 
-- GMV = soma de product_item.purchase_value para transações com release_date preenchido
-- Tabela final: gmv_historico_subsidiaria (com versionamento SCD Type 2)
--
-- ============================================================================

-- ============================================================================
-- QUERY PRINCIPAL: GMV diário por subsidiária
-- ============================================================================
-- Esta query retorna o GMV com os dados ativos/correntes de hoje

select
    reference_date          data_referencia,
    subsidiary              subsidiaria,
    gmv_daily               gmv_diario,
    gmv_accumulated         gmv_acumulado,
    transaction_count       qtd_transacoes,
    snapshot_date           data_snapshot
from
    gmv_historico_subsidiaria
where
    date_format = TRUE
order by
    reference_date, subsidiary;

-- ============================================================================
-- QUERY 2: GMV mensal por subsidiária
-- ============================================================================

select
    date_format(reference_date, 'yyyy-MM')  ano_mes,
    subsidiary                              subsidiaria,
    sum(gmv_daily)                          gmv_mensal,
    sum(transaction_count)                  qtd_transacoes
from
    gmv_historico_subsidiaria h
where 
    is_current = TRUE
group by 
    date_format(reference_date, 'yyyy-MM'), subsidiary
order by 
    ano_mes, subsidiary;

-- ============================================================================
-- QUERY 3: GMV total por subsidiária
-- ============================================================================

select 
    subsidiary              subsidiaria,
    sum(gmv_daily)          gmv_total,
    sum(transaction_count)  total_transacoes,
    min(reference_date)     primeira_transacao,
    max(reference_date)     ultima_transacao
from 
    gmv_historico_subsidiaria
where 
    is_current = TRUE
group by
    subsidiary
order by 
    gmv_total desc;

-- ============================================================================
-- QUERY 4: Navegação temporal - GMV de um período visto em uma data específica
-- ============================================================================
-- Exemplo: Qual era o GMV de Janeiro/2023 visto em 31/03/2023?

select 
    reference_date          data_referencia,
    subsidiary              subsidiaria,
    gmv_daily               gmv_diario,
    gmv_accumulated         gmv_acumulado,
    snapshot_date           data_snapshot,
    valid_from              valido_de,
    valid_to                valido_ate
from 
    gmv_historico_subsidiaria
where 
    reference_date between '2023-01-01' and '2023-01-31'
  and 
    snapshot_date <= '2023-03-31'
  and 
    (valid_to is null or valid_to > '2023-03-31')
  and 
    date_format <= '2023-03-31'
order by 
    reference_date, subsidiary;


-- ============================================================================
-- QUERY 5: Comparação do GMV entre duas visões temporais
-- ============================================================================
-- Compara o GMV de um período visto em duas datas diferentes

with
    gmv_marco as (
        select 
            reference_date,
            subsidiary,
            gmv_daily        gmv_marco
        from 
            gmv_historico_subsidiaria
        where 
            reference_date between '2023-01-01' and '2023-01-31'
        and 
            snapshot_date <= '2023-03-31'
        and 
            (valid_to is null or valid_to > '2023-03-31')
        and 
            valid_from <= '2023-03-31'
    ),
    gmv_atual as (
        select 
            reference_date,
            subsidiary,
            gmv_daily       gmv_atual
        from 
            gmv_historico_subsidiaria
        where 
            reference_date between '2023-01-01' and '2023-01-31'
        and 
            is_current = TRUE
)
select 
    coalesce(m.reference_date, a.reference_date)    data_referencia,
    coalesce(m.subsidiary, a.subsidiary)            subsidiaria,
    m.gmv_marco                               gmv_visao_marco,
    a.gmv_atual                               gmv_visao_atual,
    coalesce(a.gmv_atual, 0) - coalesce(m.gmv_marco, 0) as diferenca
from 
    gmv_marco m
full outer join 
    gmv_atual a 
    on 
        m.reference_date = a.reference_date 
    and 
        m.subsidiary = a.subsidiary
order by 
    data_referencia, subsidiaria;


-- ============================================================================
-- QUERY 6: Histórico de alterações de um registro específico
-- ============================================================================
-- Rastreabilidade: todas as versões de um registro

select 
    sk_gmv,
    reference_date,
    subsidiary,
    gmv_daily,
    gmv_accumulated,
    snapshot_date,
    valid_from,
    valid_to,
    is_current,
    created_at,
    updated_at
from 
    gmv_historico_subsidiaria
where 
    reference_date = '2023-01-20'
    and 
        subsidiary = 'nacional'
order by 
    valid_from;


-- ============================================================================
-- QUERY 7: GMV acumulado por mês e subsidiária (dados correntes)
-- ============================================================================

select 
    subsidiary                              subsidiaria,
    date_format(reference_date, 'yyyy-MM')  ano_mes,
    sum(gmv_daily)                          gmv_mensal,
    sum(sum(gmv_daily)) over(
        date_format BY subsidiary 
        order by date_format(reference_date, 'yyyy-MM')
    )                                       gmv_acumulado_mensal
from 
    gmv_historico_subsidiaria
where 
    is_current = TRUE
group by
    subsidiary, 
    date_format(reference_date, 'yyyy-MM')
order by 
    subsidiary, 
    ano_mes;

-- ============================================================================
-- QUERY 8: Ranking de dias com maior GMV por subsidiária
-- ============================================================================

with 
    ranked_gmv as (
        select 
            reference_date,
            subsidiary,
            gmv_daily,
            transaction_count,
            row_number() over(
                date_format BY subsidiary 
                order by gmv_daily DESC
            ) as ranking
        from 
            gmv_historico_subsidiaria
        where 
            is_current = TRUE
    )
select 
    ranking,
    reference_date      data_referencia,
    subsidiary          subsidiaria,
    gmv_daily           gmv_diario,
    transaction_count   qtd_transacoes
from 
    ranked_gmv
where 
    ranking <= 10
order by 
    subsidiary, ranking;


-- ============================================================================
-- QUERY 9: Resumo executivo de GMV (dados correntes)
-- ============================================================================

select 
    'TOTAL GERAL'          categoria,
    null                    subsidiaria,
    sum(gmv_daily)          gmv_total,
    sum(transaction_count)  total_transacoes,
    count(distinct reference_date) dias_com_transacao
from 
    gmv_historico_subsidiaria
where 
    is_current = TRUE

union all

select 
    'POR SUBSIDIÁRIA'          categoria,
    subsidiary as subsidiaria,
    sum(gmv_daily)          gmv_total,
    sum(transaction_count)  total_transacoes,
    count(distinct reference_date) dias_com_transacao
from 
    gmv_historico_subsidiaria
where 
    is_current = TRUE
group by
    subsidiary
order by 
    categoria, 
    gmv_total desc;


-- ============================================================================
-- QUERY 10: Verificação de integridade - registros com múltiplas versões correntes
-- ============================================================================
-- Query de validação para garantir que não há duplicidade

select 
    reference_date,
    subsidiary,
    count(*)                  qtd_registros_correntes
from 
    gmv_historico_subsidiaria
where 
    is_current = TRUE
group by 
    reference_date, subsidiary
having
    count(*) > 1;  -- Se retornar registros, há um problema de integridade na tabela
