# Data Pipeline - Inventário de Lojas com dbt e AWS Athena

## Visão Geral
Este projeto utiliza o **dbt** (Data Build Tool) em conjunto com **AWS Athena** para construir um pipeline de dados que retorna os dois últimos períodos de inventário realizados em lojas. A pipeline permite obter os dados tanto por filial quanto por produto. 

## O que é dbt?
O **dbt (Data Build Tool)** é uma ferramenta de transformação de dados que permite a engenharia de dados utilizando SQL. Com ele, é possível criar modelos de dados reutilizáveis, aplicar práticas de versionamento e executar testes de qualidade nos dados. 

Principais recursos do dbt:
- **Modelagem declarativa:** Define transformações de dados usando SQL.
- **Execução modular:** Constrói dependências entre modelos de dados automaticamente.
- **Gerenciamento de qualidade:** Permite a criação de testes para garantir a integridade dos dados.
- **Geração de documentação:** Produz documentação interativa a partir do código SQL.
- **Suporte a múltiplas plataformas:** Funciona com diversos bancos de dados, incluindo AWS Athena.

## Arquitetura do Pipeline
1. **Extração**: Os dados de inventário das lojas são carregados diariamente no AWS S3.
2. **Transformação**: Utilizando **dbt**, os dados são processados no **Athena** para consolidar os últimos dois períodos de inventário.
3. **Carga Incremental**: O processo é atualizado diariamente, considerando apenas as lojas que possuem inventário no mês corrente.
4. **Consulta e Disponibilização**: Os dados transformados ficam disponíveis no Athena para consumo por dashboards, relatórios e outros sistemas de análise.

## Tecnologias Utilizadas
- **dbt**: Para modelagem e transformação de dados.
- **AWS Athena**: Motor de consulta para processar os dados armazenados no S3.
- **AWS S3**: Armazena os dados brutos e processados.
- **Iceberg Table Format**: Utilizado para armazenamento otimizado e manutenção eficiente de dados históricos.

## Estrutura do Projeto
```
├── models
│   ├── commom
│   │   ├── cte_commom_perdas.sql
│   │   ├── cte_commom_vendas.sql
│   ├── info_produtos
│   │   ├── info_produtos.sql
│   ├── periodo_inventario
│   │   ├── periodo_inventario.sql
│   ├── perdas
│   │   ├── perdas.sql
│   ├── vendas
│   │   ├── vendas.sql
│   ├── perdas_produto
│   │   ├── perdas.sql
│   ├── vendas_produto
│   │   ├── vendas_produto.sql
│   ├── pd_periodo_inventario_filial
│   │   ├── pd_periodo_inventario_filial.sql
│   ├── pd_periodo_inventario_produto
│   │   ├── pd_periodo_inventario_produto.sql
├── dbt_project.yml             # Configuração principal do dbt
└── README.md
```

## Modelo de Carga Incremental
A carga incremental segue a seguinte lógica:
- **Utiliza a estratégia MERGE para atualizar e inserir registros**.
- **Garante a atualização eficiente dos dados sem duplicidade**.

Exemplo de modelo `pd_periodo_inventario_filial.sql`:
```sql
{{
    config(
        materialized="incremental",
        table_type="iceberg",
        incremental_strategy="merge",
        unique_key="filial",
        format="parquet",
        write_compression="ZSTD",
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
        post_hook=[
            "OPTIMIZE {{ this.render_pure() }} REWRITE DATA USING BIN_PACK",
            "VACUUM {{ this.render_pure() }}",
        ],
    )
}}

with
    pre_resumo as (
        select *
        from {{ ref("perdas") }}
        full outer join {{ ref("vendas") }} using (filial)
    ),
    pos_resumo as (
        select
            filial,
            ultimo_inventario,
            penultimo_inventario,
            ante_penultimo_inventario,
            {{ columns_rst("valor_inv") }},
            {{ columns_rst("valor_inv_ant") }},
            {{ columns_rst("venda_inv", flag=false) }},
            {{ columns_rst("venda_inv_ant", flag=false) }},
            {{ columns_perc("valor_inv") }},
            {{ columns_perc("valor_inv_ant") }},
            {{ columns_ind("valor_inv", "venda_inv") }},
            {{ columns_ind("valor_inv_ant", "venda_inv_ant") }}
        from pre_resumo
    ),
    resumo as (
        select
            *,
            case
                when ind_inv * 100 > 0.70
                then 'Extrema'
                when ind_inv * 100 > 0.40
                then 'Alta'
                when ind_inv * 100 > 0.30
                then 'Media'
                when ind_inv * 100 < 0.30
                then 'Baixa'
            end as criticidade,
            valor_inv / nullif(
                date_diff('day', penultimo_inventario, ultimo_inventario), 0
            ) as perda_diaria_inv,
            valor_inv_ant / nullif(
                date_diff('day', ante_penultimo_inventario, penultimo_inventario), 0
            ) as perda_diaria_inv_ant
        from pos_resumo
    )
    select * from resumo
```

## Como Executar o Projeto
1. **Instalar dependências**:
   ```bash
   pip install dbt-core dbt-athena
   ```
3. **Rodar o dbt**:
   ```bash
   dbt run
   ```
4. **Verificar a qualidade dos dados**:
   ```bash
   dbt test
   ```

## Monitoramento
- **dbt Docs**: Gerar documentação do modelo com:
  ```bash
  dbt docs generate && dbt docs serve
  ```
