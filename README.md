# DataEnginneeringSide1



Tecnologias :


![](/imagens/INFRA1.png) 

- Azure Datalake Gen 1
- Azure Databricks
- Delta Lake
- Scala



Dataset Utilziado do Kaggle:

- https://www.kaggle.com/antoniocgg/sao-paulo-real-estate-prediction


Tarefas:

- Criação de Usuário de serviço dp Cluster Databricks
    - Criar um AppRegister ( Usuario de serviço, registro de aplicativo(app registration))
    - ![](/imagens/appregistration.png)
        - Usuario que vai fazer a função de leitura e escrita.
        - Conexao via ouath2 do databiricks com datalake

    - Armazenar Cliente ID, Tenant ID e Secret

- Azure Data Lake Gen 1
- ![](/imagens/datalake.png)
    - Criar um Resource Group
    - Subir uma instancia de Azure Datalake Gen 1
    - ![](/imagens/datalake2.png)
    - Definir a arquitetura do Datalake
    - Criar as pastas baseadas na arquitetura definida
    - ![](/imagens/datalake3.png)
    - ![](/imagens/datalake4.png) 
    - Realizar a ingestão para a camada inicial
    - Dar permissao ao usuario de serviço

- Integrando Databricks + Datalake Gen 1
- ![](/imagens/databricks1.png) 
- ![](/imagens/cluster1.png) 
    - Criar cluster
    - Adicionar congifuração de autenticação do cluster no Datalake Gen 1
    - Testar acesso

- Criar Pipeine de Ingestão
    - Ingestão na Camada Raw/silver
    - ![](/imagens/bronze1.png) 
    - ![](/imagens/bronze2.png)


    - Ingestão na Camada Silver
    - ![](/imagens/silver1.png) 
    - ![](/imagens/silver2.png) 






- Muitas empresas ainda nao migragram para o GEN2, ainda continuam no GEN1.
- Vamos usar GEN1 porque a maioria das empresas utilizam.




- Arquitetura
    - Baseada na arquitetura DELTA LAKE

    - Camada Bronze
        - Dados que chegam são integrados na tabela bronze (parquet), sao armazenados não os dados brutos, mas sim
            os dados sem limpeza nenhuma.
        - Como o dado vai chegar ?
            - Através de uma cama de pouso ( landing zone ), ou Inbound.
            - Serão armazenados em um diretorio especifico, que sera a porta de entrada para os dados na cloud.
            - 1ª camada apaenas lemos os dados, sem nenhum tipo de limpeza.
            - Uma vez que ele esta armazenado em parquet é muito mais facil, muito mais otimizado para fazer os trabalhos de analytics, limpeza...

    - Camada SILVER
        - Dados prontos para os cientistas de dados...

    - Camada GOLD
        - Tabela mais refinada, geralmente é uma tabela mais enriquecida, para BI....modelagem especifica.
        - Não vamos chegar nela nesse projeto.

    
- FLUXO

    - INBOUND -> RAW -> SILVER -> ...


- DATA EXPLORER PARA CRIAR OS DIRETORIOS.(CRIAÇÃO DE PASTAS + UPLOAD)


- Criar permissão de acesso para APP REGISTRATION que criamos no inicio.

- Subir o serviço do databricks na azure

- Criar o cluster dentro do databricks
    - Especificar autoscaling OFF, standard, numero de instancias (dev)

- Criar um notebook para verificar acessos
    - display(dbutils.fs.ls("<url_do_datalake_raiz>"))
    - Caso retorne conforme a imagem com a estrutura de pastas, acesso está ok, e podemos seguir.



- Já no notebook...

    - val inboundFile = "adl://adlsbigdatadatabricks.azuredatalakestore.net/inbound/source-4-ds-train.json"
    - val bronzeDF = spark.read.text(inboundFile)
    - display(bronzeDF) -> Vemos o json em string....não vamos precisar de todas infos, precisamos entender o dados.

    - Tratativa de ingestão:
        - Antes de armazenar na camada Bronze, precisamos saber/entender melhro o dado.
        - Qual tratativa de ingestao, DELTA ou FULL.
        - DELTA
            - Estariamos armazenando somente o diferencial e nao a carga inteira sempre, nao necessario sobrepor a abse inteira.

        - Porque vamos usar o DELTA, temos dois campos de createdAt e updatedAt, logo devemos utilziar o range de tempo pra extrair somente o diferencial.


- ANÁLISES DOS DADOS
        - Chave única que podemos utilizar é o id.

        - import org.apache.spark.sql.functions.get_json_object 
        - bronzeDF.select(get_json_object($"value","$.id") as "id").show()
            - $. -> Sinaliza a raiz e que queremos os campos de la.
        - Podemos realizar um groupBy para ver se realmente é um id por imovel, ou se há repetições para entendermos o dado melhor.
        - bronzeDF.select(get_json_object($"value","$.id") as "id").groupBy("id").count().show()


        - Podemos fazer uma contagem e ordenar pela maior quantia encontrada para verificar se é realmente unico, usando:
            - bronzeDF.select(get_json_object($"value","$.id") as "id").groupBy("id").count().orderBy($"count".desc).show()

        - Vamos deixar o json do jeito que esta, mas vamos extrair a coluna ID pra caso em uma proxima extração venham esses IDs eles nao dupliquem.

        - Vamos utilizar o DELTA LAKE.
            - Projeto consumido pelo DATABRICKS, que transforma o data lake em um datalakeHOUSE.
            - Traz as principais propriedades de um data warehouse
                - ACID, SCHEME ENFORCEMENT, SCHEME EVOLUTION, TIME TRAVEL, coisa que nao eram possiveis com spark open source...
                - DELTA LAKE vem por cima do spark, fica junto com o spark na hora da interação do dado.

                
    
        - Criar um database usando spark sql

        - %sql CREATE DATABASE IF NOT EXISTS db_zap_project_bronze


        - Adicionamos algumas colunas na nossa landing zone (bronze)

        - Ja estamos armazenando em parquet no diretorio Bronze e o transactional log do delta.

        - CAMADA SILVER
            - Nessa camada vamos utilizar os dado sque ja estao na camada bronze (nossa landing zone)
            - Importar todas bibliotecas utilizand " ._ ", no scala conseguimos importar todas as funções do spark dentro do objeto function.
            - Feitas as congis do notebook novo do silver ja conseguimos trazer as infos.
            - Vamos utilizar um dos metodos de leitura do spark que é o read.json que depednendo da situação ele ajuda a gente a fazer o infer schema dinamico.
            - Se voce nao tem conhecimento profundo do dado, é legal usar esse tipo de tratativa que o spark ja trata automatico, se um campo nao existe ele ja trata como nulo, pra gente nesse projeto é aceitavel.
            - Só dando um select na coluna do json value é retornado um dataframe, o spark nao lida bem com isso com o read.json ele prefere que seja lgo masi tipado como um dataset de streaming, vamos converter esse cara para dataset usando asStrign (scala)
            - Vamos começar a limpar e criar um schema para disponibilizar par ao usuario.


    - ![](/imagens/silver3.png) 
    - ![](/imagens/silver4.png) 


        - Infer schema foi realizado tornando campos como pricingInfo e geoLocation em campos com um schema flat igual aos outros, muito mais facil para utilização de data scientists.






