# CDLE — Computação de Dados em Larga Escala

Este é o diretório **raiz da unidade curricular**. Slides, apontamentos, projeto, laboratórios, ambientes e dados ficam todos aqui. A reorganização começa do zero: não foram importadas soluções, medições ou scripts antigos feitos com IA.

## Organização

```text
CDLE/
├── README.md
├── .gitignore
├── materials/
│   ├── slides/         # Slides oficiais por semana
│   ├── handouts/       # Enunciados originais dos Labs 1–4
│   └── references/     # Referências facultadas pela cadeira
├── notes/
│   ├── theory/         # Apontamentos próprios por semana
│   └── labs/           # Diário de cada laboratório
├── project/            # Projeto semestral separado dos labs
└── tpcds/              # Tudo o que for executado nos Labs 1–4
    ├── README.md
    └── [pastas adicionadas durante os labs]
```

## Porquê `tpcds/` dentro de `CDLE/`?

Os quatro handouts pressupõem que o kit, os starters, os dados e os relatórios ficam lado a lado em `~/tpcds`. Aqui **essa pasta fica em `CDLE/tpcds`**. Os materiais oficiais não foram alterados: ao executares os comandos, substitui `~/tpcds` pelo caminho real de `CDLE/tpcds`, e define a variável `T` para esse caminho.

Em cada novo terminal, a partir da raiz `CDLE`:

```bash
cd '/caminho/para/CDLE'
export T="$PWD/tpcds"
export PY="$T/cdle_venv/bin/python"  # só funciona depois de criares venv
```

**Nota:** os caminhos do kit TPC-DS são sensíveis ao comprimento e a espaços. Convém que o caminho completo de `CDLE/tpcds` seja curto, sem espaços, e verificar os scripts dos starter packs quando forem instalados (podem conter caminhos literais `~/tpcds`). Não criar pastas de trabalho fora de `CDLE`.

## Método de aprendizagem

1. Recuperar os starter packs e kits oficiais, não os scripts antigos já resolvidos.
2. Fazer os exercícios por ordem, escrevendo ou modificando o código por conta própria.
3. Antes de correr uma experiência, prever o resultado; depois medir e explicar.
4. Manter as notas próprias em `notes/labs/` e as evidências/relatórios exigidos no `tpcds/report/` oficial.
5. Para componentes que requerem IA, seguir o protocolo da cadeira, declarar a utilização e verificar os resultados.

## Git e ficheiros pesados

O `.gitignore` exclui o kit proprietário TPC-DS, os ambientes virtuais, os dados e resultados volumosos/regeneráveis dentro de `tpcds`. Os materiais da cadeira e starter packs podem estar sujeitos a restrições de partilha: mantém o repositório privado e confirma as regras de publicação. Evita adicionar arquivos e dados gerados.
