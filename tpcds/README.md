# Área de execução dos laboratórios (TPC-DS)

Tudo nesta pasta pertence à unidade curricular CDLE. É o equivalente local da pasta `~/tpcds` usada nos handouts oficiais; **não** deves criar uma segunda pasta na raiz da tua conta.

## Estrutura prevista, a criar quando necessária

```text
tpcds/
├── DSGen-software-code-4.0.0/  # kit oficial do TPC-DS
├── cdle_venv/                       # Python + DuckDB
├── starter/                    # Lab 1 (starter oficial)
├── lab2/                       # Lab 2 (starter oficial)
├── lab3/                       # Lab 3 (starter oficial)
├── lab4/                       # Lab 4 (starter oficial)
├── report/                     # kit de relatórios + evidências
├── data/                       # dados brutos, regeneráveis
├── parquet/                    # datasets SF1 / SF10
├── query_templates/            # cópias corrigidas dos templates
├── queries/                    # queries geradas
└── results/                    # medições
```

## Convenção de terminal

A partir de `CDLE`, executa:

```bash
export T="$PWD/tpcds"
export PY="$T/cdle_venv/bin/python"
```

Nos handouts, `~/tpcds` corresponde a `$T`. Não alteres automaticamente os ficheiros de origem da cadeira. Quando extraíres os starter packs, verifica eventuais caminhos fixos em scripts e configurações Docker. Mantém o percurso da pasta tão curto quanto possível (o kit oficial tem limitações de caminhos).

**Espaço em disco:** SF10 ocupa muitos gigabytes. O Lab 1 permite eliminar `data/sf10/` **depois** da conversão e verificação, preservando `parquet/sf10/` para os restantes labs.
