# Laboratório 01 — TPC-DS numa única máquina

## 1. Ambiente experimental

### 1.1 Configuração do hardware

| Componente | Especificação |
|---|---|
| Computador | Apple MacBook Pro de 13 polegadas (2017) |
| Arquitetura | Intel x86-64 |
| Processador | Intel Core i5-7360U a 2,30 GHz |
| Núcleos físicos do CPU | 2 |
| Núcleos lógicos do CPU | 4 |
| Memória RAM | 8 GB |
| Armazenamento físico | SSD interno Apple, modelo `APPLE SSD AP0128J` |
| Interface de armazenamento | NVMe; ligação PCIe x4 (8,0 GT/s) |
| Capacidade nominal do SSD | 121,33 GB (121 332 826 112 bytes) |
| Capacidade indicada pelo sistema de ficheiros | 113 GiB (`df -h`) |
| Sistema de ficheiros | APFS |
| Suporte TRIM | Sim |
| Estado S.M.A.R.T. | Verificado |
| Espaço disponível inicialmente | 14 GiB |
| Sistema operativo | macOS Ventura 13.7.8 (compilação 22H730) |

**Nota:** a capacidade nominal do SSD é apresentada em GB decimais, enquanto o comando `df -h` apresenta a capacidade do sistema de ficheiros em GiB. Por esse motivo, os dois valores não são diretamente comparáveis sem conversão de unidades.

### 1.2 Ambiente de software

| Componente | Versão / configuração |
|---|---|
| Python | 3.9.6 |
| DuckDB | 1.4.5 |
| Ferramentas TPC-DS | 4.0.0 |
| Compilador C | Apple Clang |
| Ambiente virtual Python | `~/CDLE/cdle_venv` |
| Gerador de dados | `dsdgen` |
| Gerador de consultas SQL | `dsqgen` |
| Diretório principal da unidade curricular | `~/CDLE` |
| Caminho de compatibilidade com o enunciado | `~/tpcds` (ligação simbólica para o mesmo diretório) |

O laboratório é executado localmente num MacBook Pro com processador Intel. As ferramentas TPC-DS foram compiladas a partir do código-fonte com o Clang, recorrendo aos ajustes de compatibilidade com macOS indicados no enunciado do laboratório.

Os executáveis `dsdgen` e `dsqgen` foram compilados com sucesso para a arquitetura x86-64. O ficheiro `tpcds.idx`, necessário ao funcionamento das ferramentas, também foi gerado.

## 2. Geração do conjunto de dados SF1

### 2.1 Objetivo

Gerar um conjunto de dados sintéticos que representa a atividade de uma empresa de retalho, utilizando o benchmark TPC-DS com fator de escala 1 (*Scale Factor 1*, ou SF1).

Este conjunto de dados servirá de base à exploração do modelo de dados TPC-DS, à conversão dos ficheiros de texto para Parquet, à execução de consultas SQL e à realização de experiências de desempenho com o DuckDB.

### 2.2 Configuração da geração

| Parâmetro | Valor |
|---|---|
| Benchmark | TPC-DS |
| Fator de escala | SF1 |
| Ferramenta de geração | `dsdgen` 4.0.0 |
| Processos de geração em paralelo | 4 |
| Formato de saída | Ficheiros `.dat` (texto delimitado por `\|`) |
| Diretório de saída | `~/CDLE/data/sf1/` |
| Data da geração | 02/11/2026 |

O conjunto de dados foi gerado com quatro processos em paralelo. Cada processo ficou responsável por uma partição da geração, através das opções `-PARALLEL 4` e `-CHILD`, conforme previsto no enunciado.

### 2.3 Resultados medidos

| Métrica | Resultado observado |
|---|---|
| Tempo decorrido de geração (*wall-clock*) | 126,78 s (2 min 6,78 s) |
| Tempo de CPU em modo de utilizador (*user*) | 104,00 s |
| Tempo de CPU em modo de sistema (*system*) | 100,77 s |
| Utilização média de CPU indicada por `time` | 161% |
| Dimensão dos dados brutos gerados | `1.2G` (`du -sh`) |
| Número de registos de `store_sales` | 2 880 404 |
| Espaço disponível em disco após a geração | 12 GiB |

### 2.4 Validação

Os dados brutos foram gerados e guardados no diretório esperado. A validação inicial consistiu em verificar a existência dos ficheiros `.dat`, medir a dimensão do diretório de saída e contar os registos de todas as partições da tabela `store_sales`.

**Número esperado de registos de `store_sales`:** 2 880 404  
**Número observado de registos de `store_sales`:** 2 880 404

A contagem observada coincide com o valor de referência apresentado no enunciado. Esta verificação confirma a contagem de registos da tabela `store_sales`, mas não constitui, por si só, uma validação completa das restantes tabelas. Essa verificação será aprofundada durante a conversão para Parquet.

### 2.5 Observações

A geração do SF1 foi concluída com sucesso através de quatro processos de geração em paralelo, numa máquina com dois núcleos físicos e quatro núcleos lógicos de CPU.

A utilização média de CPU indicada por `time` foi de 161%, equivalente a aproximadamente 1,61 núcleos lógicos ocupados em média durante o intervalo medido. O valor não significa que cada um dos quatro processos tenha utilizado um núcleo em permanência.

Os ficheiros brutos ocupam aproximadamente `1.2G` em disco, segundo o comando `du -sh`. O espaço livre indicado por `df -h` diminuiu de 14 GiB, na verificação inicial do ambiente, para 12 GiB após a geração. Esta diferença não deve ser atribuída exclusivamente ao SF1, uma vez que também ocorreram outras operações no sistema entre as duas medições.

Dada a capacidade limitada do SSD, será necessário monitorizar o espaço disponível antes de gerar o conjunto de dados SF10, de maior dimensão.

## 3. Conversão para Parquet

*Por executar e documentar. Registar a duração da conversão, a dimensão final dos ficheiros Parquet e os resultados da validação das tabelas.*

## 4. Exploração dos dados

*Por executar e documentar.*

## 5. Execução das consultas do benchmark

*Por executar e documentar.*

## 6. Experiências de desempenho

*Por executar e documentar.*

## 7. Conclusões

*A completar após a execução e análise das experiências do laboratório.*
