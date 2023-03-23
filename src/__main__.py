#! /usr/bin/env python3

# Bibliotecas utilitárias.
import argparse
import json
import logging
import logging.config
import os
import sys
import time
from concurrent import futures
from datetime import datetime

# Adicionar pasta Generated para o caminho do módulo.
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PARENT_DIR, 'generated'))

# Bibliotecas e arquivos SSE
import grpc
from lib import ServerSideExtension_pb2 as SSE
from lib.ssedata import FunctionType
from lib.scripteval import ScriptEval

# Funções de Processamento da SSE
from functions.media_movel import media_movel
from functions.regressao_linear import regressao_linear

# Constantes
_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# Variáveis de Ambiente
sse_port = os.environ['SSE_PORT']
sse_allowscript = os.environ['SSE_ALLOWSCRIPT']

class ExtensionService(SSE.ConnectorServicer):

    def __init__(self, funcdef_file):
        """
        Classe inicializadora.
        :param funcdef_file: Arquivo JSON de definição de funções.
        """
        self._function_definitions = funcdef_file
        self.ScriptEval = ScriptEval()
        os.makedirs('logs', exist_ok=True)
        log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
        logging.config.fileConfig(log_file)
        logging.info('Logging ativado.')


    @property
    def function_definitions(self):
        """
        Função de retorno das funções do Arquivo JSON.
        :return: Arquivo JSON com as definições das funções.
        """
        return self._function_definitions

    
    @property
    def functions(self):
        """
        Função de mapeamento de todas as funções da SSE.
        :return: Mapeamento das functionId.
        """
        return{
            0: '_regressao_linear',
            1: '_media_movel'
        }

    
    @staticmethod
    def _get_function_id(context):
        """
        Recupera o functionId no header.
        :param context: Contexto.
        :return: ID da função.
        """
        metadata = dict(context.invocation_metadata())
        header = SSE.FunctionRequestHeader()
        header.ParseFromString(metadata['qlik-functionrequestheader-bin'])

        return header.functionId

    """
    Implementação das chamadas de Funções do Plugin.
    """

    @staticmethod
    def _regressao_linear(request, context):
        response_rows = regressao_linear(request, context)
        yield SSE.BundledRows(rows=response_rows)

    @staticmethod
    def _media_movel(request, context):
        response_rows = media_movel(request, context)
        yield SSE.BundledRows(rows=response_rows)

    """
    Implementação das funções RPC
    """

    def GetCapabilities(self, request, context):
        """
        """
        logging.info('GetCapabilities')
        # Cria uma instância de Capabilities em mensagem grpc
        # Ativa ou desativa a avaliação de script
        # Seta valores para pluginIndentifier and pluginVersion
        capabilities = SSE.Capabilities(allowScript=True if sse_allowscript == 'true' else False,
                                        pluginIdentifier='QLIK-SSE',
                                        pluginVersion='1.0.0')
        
        # Caso suporte a definição das funções do usuário, adiciona as definições na mensagem.
        with open(self.function_definitions) as json_file:
            # Iterar cada definição de função e adicionar dados para a mensagem dos recursos gRPC.
            for definition in json.load(json_file)['Functions']:
                function = capabilities.functions.add()
                function.name = definition['Name']
                function.functionId = definition['Id']
                function.functionType = definition['Type']
                function.returnType = definition['ReturnType']

                # Recupera o nome e o tipo de cada parâmetro
                for param_name, param_type in sorted(definition['Params'].items()):
                    function.params.add(name=param_name, dataType=param_type)

                logging.info('Adicionando aos recursos: {}({})'.format(function.name,
                                                                        [p.name for p in
                                                                        function.params]))

        return capabilities


    def ExecuteFunction(self, request_iterator, context):
        """
        Executa o chamado da função.
        :param request_iterator: Uma sequência iterável de linhas.
        :param context: Contexto.
        :return: Uma sequência iterável de linhas.
        """
        # Recupera o Id da função.
        func_id = self._get_function_id(context)

        # Chama a função correspondente.
        logging.info('ExecuteFunction (functionId: {})'.format(func_id))

        return getattr(self, self.functions[func_id])(request_iterator, context)

    
    def EvaluateScript(self, request, context):
        """
        Chamada de funcionalidade apenas pra chamada de scripts sem parâmetros e chamadas de scripts tensor.
        """
        # Analisa o header para o request do script.
        metadata = dict(context.invocation_metadata())
        header = SSE.ScriptRequestHeader()
        header.ParseFromString(metadata['qlik-scriptrequestheader-bin'])

        # Recupera o tipo da função.
        func_type = self.ScriptEval.get_func_type(header)

        # Verifica o tipo da função.
        if(func_type == FunctionType.Aggregation) or (func_type == FunctionType.Tensor):
            return self.ScriptEval.EvaluateScript(header, request, context, func_type)
        else:
            msg = 'Tipo de Função {} não é suportada neste plugin.'.format(func_type.name)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(msg)
            raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)

    """
    Implementação da conexão do servidor com gRPC
    """

    def initServer(self, port, pem_dir):
        """
        Configurações do Servidor gRPC.
        :param port: Porta de escuta do Servidor.
        :param pem_dir: Diretório dos certificados para conexão segura.
        :return: none
        """
        # Criação do Servidor gRPC
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        SSE.add_ConnectorServicer_to_server(self, server)

        if pem_dir:
            # Conexão Segura
            with open(os.path.join(pem_dir, 'sse_server_key.pem'), 'rb') as f:
                private_key = f.read()
            with open(os.path.join(pem_dir, 'sse_server_cert.pem'), 'rb') as f:
                cert_chain = f.read()
            with open(os.path.join(pem_dir, 'root_cert.pem'), 'rb') as f:
                root_cert = f.read()
            credentials = grpc.ssl_server_credentials([(private_key, cert_chain)], root_cert, True)
            server.add_secure_port('[::]:{}'.format(port), credentials)
            logging.info('*** Executando em modo seguro através da porta: {} ***'.format(port))
        else:
            # Conexão Insegura
            server.add_insecure_port('[::]:{}'.format(port))
            logging.info('*** Executando em modo inseguro através da porta: {} ***'.format(port))

        # Iniciar o Servidor gRPC.
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', nargs='?', default=sse_port)
    parser.add_argument('--pem_dir', nargs='?')
    parser.add_argument('--definition_file', nargs='?', default='functions.json')
    args = parser.parse_args()

    def_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.definition_file)

    calc = ExtensionService(def_file)
    calc.initServer(args.port, args.pem_dir)