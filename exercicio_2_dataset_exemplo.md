# Exercício 2 - Exemplo do Dataset Final Populado

---

## Dados de Entrada (Eventos CDC)

### Tabela: purchase

| transaction_datetime | transaction_date | purchase_id | buyer_id | prod_item_id | order_date | release_date | producer_id |
|---------------------|------------------|-------------|----------|--------------|------------|--------------|-------------|
| 2023-01-20 22:00:00 | 2023-01-20 | 55 | 15947 | 5 | 2023-01-20 | 2023-01-20 | 852852 |
| 2023-01-26 00:01:00 | 2023-01-26 | 56 | 369798 | 746520 | 2023-01-25 | NULL | 963963 |
| 2023-02-05 10:00:00 | 2023-02-05 | 69 | 160001 | 5 | 2023-01-20 | 2023-01-20 | 852852 |
| 2023-02-26 03:00:00 | 2023-02-26 | 69 | 160001 | 18 | 2023-02-26 | 2023-02-28 | 96967 |
| 2023-07-15 09:00:00 | 2023-07-15 | 55 | 160001 | 5 | 2023-01-20 | 2023-03-01 | 852852 |

### Tabela: product_item

| transaction_datetime | transaction_date | prod_item_id | prod_item_partition | product_id | item_quantity | purchase_value |
|---------------------|------------------|--------------|---------------------|------------|---------------|----------------|
| 2023-01-20 21:00:00 | 2023-01-20 | 5 | NULL | 101 | 1 | 150.00 |
| 2023-01-25 20:00:00 | 2023-01-25 | 746520 | NULL | 102 | 1 | 200.00 |
| 2023-02-26 02:00:00 | 2023-02-26 | 18 | NULL | 103 | 1 | 350.00 |

### Tabela: purchase_extra_info

| transaction_datetime | transaction_date | purchase_id | purchase_partition | subsidiary |
|---------------------|------------------|-------------|-------------------|------------|
| 2023-01-23 00:05:00 | 2023-01-23 | 55 | NULL | nacional |
| 2023-01-25 23:59:59 | 2023-01-25 | 56 | NULL | internacional |
| 2023-02-28 01:10:00 | 2023-02-28 | 69 | NULL | nacional |
| 2023-03-12 07:00:00 | 2023-03-12 | 69 | NULL | internacional |

---

## Relacionamentos (conforme diagrama dbdiagram)

```
purchase.prod_item_id ──────> product_item.prod_item_id
purchase.purchase_id <────── purchase_extra_info.purchase_id
```

---

## Interpretação dos Dados de Entrada

### Análise por purchase_id:

**purchase_id = 55:**
- Primeira versão (2023-01-20): release_date = 2023-01-20 ✓ (liberado)
- Última versão (2023-07-15): release_date = 2023-03-01 (atualização retroativa)
- prod_item_id = 5 → purchase_value = R$ 150,00
- Subsidiária: nacional (conforme extra_info de 2023-01-23)
- **Valor para GMV: R$ 150,00**

**purchase_id = 56:**
- Única versão (2023-01-26): release_date = NULL ✗ (não liberado)
- prod_item_id = 746520 → purchase_value = R$ 200,00
- Subsidiária: internacional
- **Valor para GMV: R$ 0,00** (não conta pois release_date é NULL)

**purchase_id = 69:**
- Primeira versão (2023-02-05): prod_item_id = 5, release_date = 2023-01-20 ✓
- Última versão (2023-02-26): prod_item_id = 18, release_date = 2023-02-28 ✓
- Subsidiária: nacional → internacional (mudou em 2023-03-12)
- **Valor para GMV: R$ 350,00** (usando última versão: prod_item_id=18, subsidiária mais recente)

---

## Lógica de Deduplicação CDC

Para cada tabela, mantemos apenas o registro mais recente por chave natural:

| Tabela | Chave Natural | Ordenação |
|--------|---------------|-----------|
| purchase | purchase_id | transaction_datetime DESC |
| product_item | prod_item_id | transaction_datetime DESC |
| purchase_extra_info | purchase_id | transaction_datetime DESC |

### Após deduplicação:

**purchase (mais recente por purchase_id):**
| purchase_id | prod_item_id | order_date | release_date | producer_id |
|-------------|--------------|------------|--------------|-------------|
| 55 | 5 | 2023-01-20 | 2023-03-01 | 852852 |
| 56 | 746520 | 2023-01-25 | NULL | 963963 |
| 69 | 18 | 2023-02-26 | 2023-02-28 | 96967 |

**purchase_extra_info (mais recente por purchase_id):**
| purchase_id | subsidiary |
|-------------|------------|
| 55 | nacional |
| 56 | internacional |
| 69 | internacional |

---

## Dados Consolidados (após JOIN)

| purchase_id | prod_item_id | order_date | release_date | purchase_value | subsidiary |
|-------------|--------------|------------|--------------|----------------|------------|
| 55 | 5 | 2023-01-20 | 2023-03-01 | 150.00 | nacional |
| 56 | 746520 | 2023-01-25 | NULL | 200.00 | internacional |
| 69 | 18 | 2023-02-26 | 2023-02-28 | 350.00 | internacional |

---

## Cálculo do GMV

**Filtro**: Apenas transações com `release_date IS NOT NULL`

| purchase_id | order_date | purchase_value | subsidiary | Entra no GMV? |
|-------------|------------|----------------|------------|---------------|
| 55 | 2023-01-20 | 150.00 | nacional | ✅ SIM |
| 56 | 2023-01-25 | 200.00 | internacional | ❌ NÃO (release_date NULL) |
| 69 | 2023-02-26 | 350.00 | internacional | ✅ SIM |

**GMV Diário por Subsidiária:**

| reference_date | subsidiary | gmv_daily | transaction_count |
|---------------|------------|-----------|-------------------|
| 2023-01-20 | nacional | 150.00 | 1 |
| 2023-02-26 | internacional | 350.00 | 1 |

---

## Dataset Final: gmv_historico_subsidiaria

### Snapshot de 2023-07-15 (versão corrente/atual)

| sk_gmv | reference_date | subsidiary | gmv_daily | gmv_accumulated | transaction_count | snapshot_date | valid_from | valid_to | is_current |
|--------|---------------|------------|-----------|-----------------|-------------------|---------------|------------|----------|------------|
| 2023-01-20_nacional_2023-07-15 | 2023-01-20 | nacional | 150.00 | 150.00 | 1 | 2023-07-15 | 2023-07-15 | NULL | TRUE |
| 2023-02-26_internacional_2023-07-15 | 2023-02-26 | internacional | 350.00 | 350.00 | 1 | 2023-07-15 | 2023-07-15 | NULL | TRUE |

---

## Demonstração de Navegação Temporal

### Consulta: "Qual é o GMV diário por subsidiária (dados correntes)?"

```sql
SELECT 
    reference_date,
    subsidiary,
    gmv_daily,
    gmv_accumulated
FROM gmv_historico_subsidiaria
WHERE is_current = TRUE
ORDER BY reference_date, subsidiary;
```

**Resultado:**

| reference_date | subsidiary | gmv_daily | gmv_accumulated |
|---------------|------------|-----------|-----------------|
| 2023-01-20 | nacional | 150.00 | 150.00 |
| 2023-02-26 | internacional | 350.00 | 350.00 |

---

## Observações Importantes

1. **GMV vem de product_item.purchase_value**: O valor monetário é obtido através do join `purchase.prod_item_id = product_item.prod_item_id`

2. **Filtro de release_date**: Apenas transações com `release_date IS NOT NULL` entram no cálculo do GMV

3. **Deduplicação CDC**: Cada tabela é deduplicada mantendo o registro mais recente por chave natural

4. **Chegada assíncrona**: Os dados podem chegar em momentos diferentes, por isso usamos LEFT JOIN

5. **Subsidiária mais recente**: Em caso de múltiplas versões de `purchase_extra_info`, usamos a mais recente

---

## Resumo do GMV (Dados Correntes)

| Mês | Subsidiária | GMV Mensal | Transações |
|-----|-------------|------------|------------|
| Janeiro/2023 | nacional | R$ 150,00 | 1 |
| Fevereiro/2023 | internacional | R$ 350,00 | 1 |
| **TOTAL** | - | **R$ 500,00** | **2** |

> **Nota**: A compra 56 (R$ 200,00) não foi contabilizada pois não teve pagamento liberado (release_date = NULL).
