# De Framework a Performance: Construindo e Otimizando um Pipeline ETL com PySpark

Este repositório é um estudo de caso completo sobre o ciclo de vida de um pipeline de dados. Ele detalha o processo de ponta a ponta, começando pela construção de um **framework ETL robusto e rico em funcionalidades** e culminando na sua **otimização de performance** para um ambiente de produção simulado.

O objetivo é servir como um guia prático que não apenas mostra como construir um pipeline que funciona, mas também como torná-lo eficiente, escalável e econômico, abordando os desafios do mundo real enfrentados por engenheiros de dados.

## Parte 1: A Construção de um Framework Robusto (O Ponto de Partida)

Antes de otimizar, é preciso ter uma base sólida. A primeira fase deste projeto focou na criação de um pipeline que fosse mais do que um simples script, mas sim um framework reutilizável, seguindo as melhores práticas de engenharia de software.

O cenário utilizado foi a transformação de dados transacionais do dataset **Olist E-Commerce** em um modelo dimensional **Star Schema**, pronto para análises.

### Arquitetura e Funcionalidades do Framework Base

As principais características que garantem a robustez e a qualidade do nosso ponto de partida são:

  * **Arquitetura Orientada a Objetos:** Toda a lógica do pipeline é encapsulada na classe `AdvancedStarSchemaPipeline`, promovendo organização, reusabilidade e facilidade de manutenção.
  * **Configuração Externa via YAML:** Todas as configurações (caminhos, nomes de tabelas, parâmetros de DQ) são gerenciadas em um arquivo `config.yaml`, permitindo que o pipeline seja executado em diferentes ambientes (desenvolvimento, produção) sem nenhuma alteração no código.
  * **Framework de Qualidade de Dados (Data Quality):** Foram implementadas validações automáticas para garantir a confiabilidade dos dados, incluindo:
      * Verificação de unicidade e não nulidade de chaves primárias.
      * Testes de integridade referencial entre tabelas fato e dimensão.
      * Validação de valores aceitáveis para colunas categóricas (ex: `order_status`).
  * **Perfil de Dados Automatizado (Data Profiling):** O pipeline gera automaticamente um relatório em JSON com estatísticas descritivas das tabelas mais importantes. Isso é crucial para monitorar a saúde dos dados e detectar anomalias ou desvios (data drift) ao longo do tempo.
  * **Modelo Dimensional Extensível:** O design foi pensado para ser facilmente expansível, com a inclusão de múltiplas dimensões como `Dim_Cliente`, `Dim_Produto`, e a mais complexa `Dim_Geolocalizacao`.

## Parte 2: A Transição para a Otimização

Com um framework robusto e funcional em mãos, o próximo passo no ciclo de vida de um produto de dados é garantir que ele performe bem em larga escala. A execução do pipeline com volumes de dados crescentes revelaria gargalos de performance, principalmente em etapas de junção de dados e recálculos desnecessários.

**Um pipeline lento não apenas atrasa a entrega de dados, mas também gera custos mais altos em ambientes de nuvem.** A seção seguinte aborda exatamente como identificar e resolver esses gargalos.

## Parte 3: Otimização de Performance em Larga Escala

Nesta fase, refatoramos o pipeline aplicando duas das técnicas de otimização mais eficazes do PySpark.

### ⚡ Técnica 1: Caching de DataFrames (`.cache()`)

#### O Problema: Recomputação Desnecessária

Devido à natureza de "execução preguiçosa" (lazy evaluation) do Spark, se um DataFrame complexo for usado em múltiplas ações (ex: uma vez para construir a tabela de fatos e outra para uma consulta), o Spark pode re-executar todo o plano de cálculo para gerá-lo a cada vez, desperdiçando recursos.

#### A Solução: Persistência em Memória

O método `.cache()` instrui o Spark a armazenar o DataFrame em memória após sua primeira computação. Nas utilizações seguintes, os dados são lidos diretamente da memória, o que é ordens de magnitude mais rápido. É crucial chamar uma **ação** (como `.count()`) após o `.cache()` para forçar sua materialização.

#### Exemplo de Código (Antes e Depois)

```python
# --- CÓDIGO ANTERIOR (Robusto, mas não otimizado para reuso) ---
def _create_dimensions_sem_cache(self):
    # ...
    # Esta dim_produto seria recalculada se usada em outra ação posterior.
    dim_produto = df_products.join(...).distinct()
    self.dimensional_models["Dim_Produto"] = dim_produto

# --- CÓDIGO OTIMIZADO (Com Cache) ---
def _create_dimensions_com_cache(self):
    # ...
    # A lógica de criação é seguida por .cache()
    dim_produto = df_products.join(...).distinct().cache()
    
    # Chamamos uma ação para forçar a materialização do cache
    self.logger.info(f"Dim_Produto criada com {dim_produto.count()} registros. Cacheada.")
    self.dimensional_models["Dim_Produto"] = dim_produto
```

#### Impacto Aproximado

  * **Tempo de Execução:** Redução massiva no tempo de jobs que reutilizam DataFrames.
  * **Recursos (CPU/I/O):** Grande economia de recursos, pois o plano de execução não é re-executado.
  * **Custo de Nuvem:** Redução direta nos custos (ex: AWS EMR, Databricks).

### ⚡ Técnica 2: Broadcast Joins (`broadcast()`)

#### O Problema: O "Shuffle" de Dados

Ao unir uma tabela grande (fatos) com uma tabela menor (dimensão), a estratégia padrão do Spark (`Shuffle Join`) precisa embaralhar os dados da tabela grande pela rede, uma das operações mais lentas e custosas em sistemas distribuídos.

#### A Solução: Enviar a Tabela Pequena para Todos

Com um **Broadcast Join**, em vez de mover a tabela grande, o Spark envia uma cópia completa da tabela pequena para cada nó do cluster. A junção ocorre localmente em cada nó, eliminando o shuffle da tabela grande. A função `broadcast()` é usada para instruir o Spark a adotar essa estratégia.

#### Exemplo de Código (Antes e Depois)

```python
from pyspark.sql.functions import broadcast

# --- CÓDIGO ANTERIOR (Funcional, mas propenso a Shuffle) ---
def _create_fact_table_sem_broadcast(self):
    # Spark provavelmente usaria um Shuffle Join, movendo dados de Fato e Dimensão.
    fato_com_chaves = base_fato.join(self.dimensional_models["Dim_Produto"], "id_produto", "left")
    return fato_com_chaves

# --- CÓDIGO OTIMIZADO (Com Broadcast) ---
def _create_fact_table_com_broadcast(self):
    # Forçamos o broadcast da tabela de dimensão (pequena)
    fato_com_chaves = base_fato.join(
        broadcast(self.dimensional_models["Dim_Produto"]), 
        base_fato.product_id == self.dimensional_models["Dim_Produto"].id_negocio_produto, 
        "left"
    )
    return fato_com_chaves
```

#### Impacto Aproximado

  * **Performance:** Aumento drástico na velocidade de joins, podendo reduzir o tempo de horas para minutos.
  * **Recursos de Rede:** Eliminação do tráfego de rede massivo associado ao shuffle.
  * **Estabilidade:** Reduz a chance de erros de memória e torna o job mais estável.
  * **Cuidado:** Use apenas quando uma das tabelas for pequena o suficiente para caber na memória de cada nó executor.
