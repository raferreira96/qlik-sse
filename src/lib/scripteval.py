import logging
import logging.config

import grpc
from lib.ssedata import ArgType, ReturnType, FunctionType

import lib.ServerSideExtension_pb2 as SSE

class ScriptEval:
    # Classe para funcionalidade ScriptEval do Plugin SSE.
    def EvaluateScript(self, header, request, context, func_type):
        """
        Avalia o script fornecido no header, dados os argumentos fornecidos na sequência dos objetos RowData, o request.
        :param header:
        :param request: Uma sequência iterável de RowData.
        :param context: O contexto enviado pelo Client.
        :param func_type: Tipo da Função.
        :return: Uma sequência iterável de RowData.
        """
        # Recupera os tipos de dados do header
        arg_types = self.get_arg_types(header)
        ret_type = self.get_return_type(header)

        logging.info('EvaluateScript: {} ({} {}) {}'.format(header.script, arg_types, ret_type, func_type))

        aggr = (func_type == FunctionType.Aggregation)

        # Checa se foram fornecidos parâmetros.
        if header.params:
            # Verfica o tipo do argumento.
            if arg_types == ArgType.String:
                # Cria uma lista vazia se for função Tensor.
                if aggr:
                    all_rows = []

                # Iterado em Linhas Agrupadas
                for request_rows in request:
                    # Iterado em Linhas
                    for row in request_rows.rows:
                        # Recupera dados numéricos para duals
                        params = self.get_arguments(context, arg_types, row.duals)

                        if aggr:
                            # Acrescenta valores na lista.
                            all_rows.append(params)
                        else:
                            # Avalia a linha de script
                            yield self.evaluate(context, header.script, ret_type, params=params)
                
                # Avalia o script baseado no dado de todas as linhas.
                if aggr:
                    params = [list(param) for param in zip(*all_rows)]
                    yield self.evaluate(context, header.script, ret_type, params=params)
            else:
                # Este plugin não suporta outros tipos de argumentos do que String.
                # Certifique-se que o tratamento de erros, incluindo o logging, funcionem de forma adequada no Client.
                msg = 'Tipo de Argumento: {} não é suportado neste plugin.'.format(arg_types)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(msg)
                # Lança o erro para o plugin-side.
                raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)
        else:
            # Este plugin não suporta avaliação de script sem parâmetros.
            # Certifique-se que o tratamento de erros, incluindo o logging, funcionem de forma adequada no Client.
            msg = 'Avaliação de Script sem parâmetros não é suportada neste plugin.'
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(msg)
            # Lança o erro para o plugin-side.
            raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)
    
    
    @staticmethod
    def get_arguments(context, arg_types, duals):
        """
        Pega o array de argumentos com base em duals, e tipos (string, numeric) especificados no header.
        :param context: o contexto enviado do client.
        :param arg_types: Tipos de Argumentos.
        :param duals: Uma sequência iterável de duals.
        :return: Lista de argumentos string.
        """
        if arg_types == ArgType.String:
            # Todos os parâmetros são do Tipo String.
            script_args = [d.strData for d in duals]
        else:
            # Este plugin não suporta outro tipo de argumentos que não sejam string.
            # Certifique-se que o tratamento de erros, incluindo o logging, funcionem de forma adequada no Client.
            msg = 'Tipo de Argumento {} não é suportado neste plugin.'.format(arg_types)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(msg)
            # Lança o erro para o plugin-side.
            raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)

        return script_args


    @staticmethod
    def get_arg_types(header):
        """
        Determina o tipo de argumento de todos os parâmetros.
        :param header:
        :return ArgType
        """
        data_types = [param.dataType for param in header.params]

        if not data_types:
            return ArgType.Empty
        elif len(set(data_types)) > 1 or all(data_type == SSE.DUAL for data_type in data_types):
            return ArgType.Mixed
        elif all(data_type == SSE.STRING for data_type in data_types):
            return ArgType.String
        elif all(data_type == SSE.NUMERIC for data_type in data_types):
            return ArgType.Numeric
        else:
            return ArgType.Undefined


    @staticmethod
    def get_return_type(header):
        """
        :param header:
        :return: Tipo do Retorno
        """
        if header.returnType == SSE.STRING:
            return ReturnType.String
        elif header.returnType == SSE.NUMERIC:
            return ReturnType.Numeric
        elif header.returnType == SSE.DUAL:
            return ReturnType.Dual
        else:
            return ReturnType.Undefined


    @staticmethod
    def evaluate(context, script, ret_type, params=[]):
        """
        Avalia um script com os parâmetros dados.
        :param context: O contexto enviado do client.
        :param script: Script para avaliar.
        :param ret_type: Retorna o tipo de dado.
        :param params: Parâmetros para avaliar. Default: []
        :return: uma RowData de string dual
        """
        if ret_type == ReturnType.String:
            # Avalia o Script
            result = eval(script, {'args': params})
            # Transforma o resultado em um iterável de dado Dual com valor de string
            duals = iter([SSE.Dual(strData=result)])
            # Cria uma linha de dados fora do duals.
            return SSE.BundledRows(rows=[SSE.Row(duals=duals)])
        else:
            # Este plugin não suporta outro tipo de retorno de dados do que string.
            # Certifique-se que o tratamento de erros, incluindo o logging, funcionem de forma adequada no Client.
            msg = 'Tipo de Retorno {} não é suportado neste plugin.'.format(ret_type)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(msg)
            # Lança o erro no plugin-side.
            raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)