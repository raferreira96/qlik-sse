# Qlik-SSE

Qlik Server Side Extension desenvolvida para conclusão de curso no Programa de Residência em TI da Universidade Federal do Rio Grande do Norte (UFRN) em parceria com o Tribunal Regional Federal da 5ª Região (TRF5). Baseado no repositório [qlik-oss/server-side-extension](https://www.github.com/qlik-oss/server-side-extension).

## Sobre

Server Side Extension em um plugin escrito em `Python` com `gRPC` usado no processamento de dados do Qlik Sense do Tribunal Regional Federal da 5ª Região (TRF5) com bibliotecas de data science e machine learning: `pandas`, `numpy` , `scikit-learn`, `tensorflow` e outras.

## Pré-requisitos e Instalação

Você pode utilizar esta SSE como uma instalação local ou executando uma imagem de contêiner.

### Docker Image

Uma imagem desta SSE está disponível em [Docker Hub](https://hub.docker.com/r/raferreira96/qlik-sse).

Para fazer o pull diretamente do Docker Hub:

```
docker build -t raferreira96/qlik-sse .
```

A imagem utiliza por padrão a porta 50050:

```
docker run -p 50050:50050 -it raferreira96/qlik-sse
```

Para executa o contêiner em detached mode, com reinício automático e armazenamento de logs em volumes:

```
docker run \
    -p 50050:50050 -d \
    --restart unless-stopped \
    -v $PWD/src:/sse/src \
    -v $PWD/logs:/sse/logs \
    raferreira96/qlik-sse
```

### Executando em um Servidor

Para instalar em um servidor linux, você pode copiar os arquivos da pasta `src`, colar na pasta desejada e executar o comandos `python src` para executar pelo diretório ou `python __main__.py` caso esteja no diretório `src`.

Requisites:

- **SO:** Linux. Recommended Debian 11 Bullseye.
- **Python:** 3.10 >=.
- **Pip**.
- **Requeriments on `requeriments.txt` in `src`**.
