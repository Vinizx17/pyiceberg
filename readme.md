# Projeto ETL PyIceberg: CSV para Data Lake (**MinIO** & **Hive Metastore**)

Este projeto implementa um pipeline de **ETL** (Extract, Transform, Load) utilizando Python e a biblioteca **PyIceberg** para ler dados brutos em formato CSV, aplicar transformações e persistir o resultado em um formato de tabela **Iceberg** em um Data Lake hospedado no **MinIO**, utilizando o **Hive Metastore** como catálogo.

## Stack Tecnológica

| Componente | Tecnologia | Função no Pipeline |
| :--- | :--- | :--- |
| **Ambiente de Desenvolvimento** | **Jupyter Notebook** | Execução interativa e orquestração do pipeline. |
| **Manipulação de Dados** | **Python**, **Pandas**, **PyArrow** | Limpeza e conversão de dados. |
| **Armazenamento** | **MinIO (S3 API)** | Armazena dados brutos (`RAW_BUCKET`) e tabelas Iceberg (`ICEBERG_BUCKET`). |
| **Catálogo de Metadados** | **Apache Hive Metastore** | Gerencia os metadados e o *schema* das tabelas **Iceberg**. |
| **Formato de Tabela** | **Apache Iceberg** | Formato de tabela aberta que garante consistência e evolução de *schema*. |
| **Conectividade** | **PyIceberg**, **Boto3** | Interface entre Python, o catálogo Hive e o armazenamento S3 (MinIO). |

## Como Iniciar

Este projeto é projetado para ser executado em um ambiente com Docker Compose. Seguir o comando abaixo no powershell para subida do ambiente

```bash
docker-compose up --build -d

### Pré-requisitos

Visual studio
Docker desktop

