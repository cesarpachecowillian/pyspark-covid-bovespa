# Big Data com PySpark: COVID-19 no Brasil e histórico da BOVESPA

Três notebooks que respondem perguntas analíticas sobre bases de **milhões de registros** com Apache Spark, usando as três camadas da API: **RDD**, **Spark SQL / DataFrame** e **Spark SQL com visualização**.

Atividades da disciplina **Big Data Analytics** da Especialização em Inteligência Artificial e Ciência de Dados da PUCPR (Prof. Eduardo Viegas).

![Casos positivos por dia no Paraná e em Santa Catarina](figuras/06_positivos_dia_pr_sc.png)

## Notebooks

| Notebook | Base | API | Perguntas |
|---|---|---|---|
| [`01_spark_rdd_covid.ipynb`](notebooks/01_spark_rdd_covid.ipynb) | exames de COVID-19 no Brasil (amostra de 1 milhão de pacientes) | RDD: `map`, `filter`, `flatMap`, `reduceByKey`, `join`, `subtract` | 21 |
| [`02_spark_sql_bovespa.ipynb`](notebooks/02_spark_sql_bovespa.ipynb) | negociações da BOVESPA de 1986 a 2019 (8,1 milhões de registros) | DataFrame e SQL, cada item resolvido das duas formas | 16 |
| [`03_spark_sql_visualizacao_covid.ipynb`](notebooks/03_spark_sql_visualizacao_covid.ipynb) | exames de COVID-19 no Brasil (4,4 milhões de pacientes) | Spark SQL + Matplotlib | 11 gráficos |

## Abordagem

* **Definições explícitas antes das consultas.** Cada notebook declara o que conta como "positivo", "óbito", "operação" ou "papel", porque a resposta muda conforme a definição.
* **Limpeza dentro do Spark**: valores ausentes (`''`, `null`, `undefined`), municípios gravados com e sem acento, sintomas com grafias diferentes (`Dispneia`, `Dispnéia`, `Dispineia`) e idades impossíveis (179 anos) são tratados antes da agregação.
* **O Spark sumariza, o driver só desenha.** Nenhum gráfico é feito a partir dos milhões de linhas: cada consulta termina em `groupBy(...).agg(...)` e só o resumo vem para o Matplotlib.
* **Mesmo resultado por dois caminhos.** No notebook da BOVESPA cada item tem a versão em API de DataFrame e a consulta SQL equivalente sobre uma visão temporária; ambas passam pelo otimizador Catalyst.
* **Leitura crítica dos dados.** As respostas apontam quando o número reflete a cobertura da base e não o fenômeno (por exemplo, Santa Catarina tem 15 vezes mais positivos que o Paraná porque o Paraná está sub-representado) e quando a unidade da base engana (preços da BOVESPA em lotes de mil ações e em moedas como cruzeiro e cruzado).

## Alguns resultados

**COVID-19 (RDD, 1 milhão de pacientes)**
* 277.848 positivos (27,8%); entre eles, tosse (46,8%) e febre (37,5%) são os sintomas mais relatados.
* Segunda-feira é o dia com mais testes e mais positivos; o volume despenca no fim de semana.
* A taxa de óbito cresce com a idade: abaixo de 0,1% de 10 a 49 anos e acima de 1,5% a partir dos 70.

**BOVESPA (Spark SQL)**
* A PETR4 teve operações em 5.391 pregões, de 1998 a 2019.
* 183.127 papéis distintos; o real responde por 86% dos registros, seguido de cruzeiro, cruzado e cruzado novo.
* Médias e desvios anuais da PETR4 e das 10 ações mais negociadas, com tabela dinâmica (pivot).

**COVID-19 (visualização, 4,4 milhões de pacientes)**

<p>
<img src="figuras/01_top10_sintomas_positivos.png" width="49%" alt="Sintomas mais comuns entre positivos">
<img src="figuras/11_idades_pr_evolucao.png" width="49%" alt="Idade por evolução do caso no Paraná">
</p>

O pico de exames foi em 13/07/2020, com 98.871 pacientes em um único dia; a idade é a variável que mais separa desfechos leves de internações e óbitos.

## Como executar

Os notebooks foram feitos para o **Google Colab** e não dependem de nenhum arquivo deste repositório: a primeira célula instala o PySpark e a terceira baixa a base com `gdown`. Basta abrir o notebook no Colab e executar todas as células.

Para rodar localmente:

```bash
pip install -r requirements.txt   # requer Java 8, 11 ou 17 instalado
jupyter notebook notebooks/
```

## Dados

As bases foram disponibilizadas pelo professor e são baixadas automaticamente pelos notebooks:

| Arquivo | Conteúdo | Tamanho aproximado |
|---|---|---|
| `covid_sample.csv` | amostra de 1 milhão de pacientes testados para COVID-19 até 2020 | 370 MB |
| `covid.csv` | base completa, 4,4 milhões de pacientes | 1,6 GB |
| `bovespa.csv` | negociações diárias da BOVESPA, 8,1 milhões de registros | 1,3 GB |

Por causa do tamanho (o GitHub limita arquivos a 100 MB), as bases não estão versionadas aqui. As saídas de todas as células ficaram salvas nos notebooks, então os resultados podem ser lidos sem executar nada.

## Tecnologias

Python, Apache Spark 3.5 (PySpark: RDD, DataFrame, Spark SQL), Matplotlib, pandas, Google Colab.

## Autor

**César Willian Pacheco**
