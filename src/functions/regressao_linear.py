import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from lib import ServerSideExtension_pb2 as SSE

def regressao_linear(request, context):

    date_list = []
    value_list = []
    for request_rows in request:

        for row in request_rows.rows:
            date_list.append([d.strData for d in row.duals][0])
            value_list.append([d.strData for d in row.duals][1])

    df = pd.DataFrame({'data_protocolo':date_list,'numero_processo':value_list})
    df['quantidade'] = 1

    df['data_protocolo'] = pd.to_datetime(df['data_protocolo'])
    df = df.sort_values(by=['data_protocolo'])
    df = df.drop_duplicates(subset='numero_processo')
    df.drop(['numero_processo'], axis=1, inplace=True)

    df = df.resample('M', on='data_protocolo').agg({'quantidade':np.sum})

    data_list = df['quantidade'].values.tolist()

    x_len = len(data_list)
    x = np.asarray(range(x_len))

    y = np.asarray(data_list)

    mdl = LinearRegression().fit(x.reshape(-1, 1),y)

    m = mdl.coef_[0]
    b = mdl.intercept_
    
    result_list = []
    gen = (i for i in range(x_len))

    for i in gen:
        y = m * i + b
        result_list.append(y)

    df = df.reset_index()
    df.columns = ['data','quantidade']
    df['regressao'] = result_list
    df['data'] = df['data'].astype(str)
    df['quantidade'] = df['quantidade'].astype(str)
    df['regressao'] = df['regressao'].astype(str)

    date_list = df['data'].values.tolist()
    quantidade_list = df['quantidade'].values.tolist()
    regressao_list = df['regressao'].values.tolist()
    
    duals_list = []
    duals_list.append([SSE.Dual(strData=d) for d in date_list])
    duals_list.append([SSE.Dual(strData=d) for d in quantidade_list])
    duals_list.append([SSE.Dual(strData=d) for d in regressao_list])

    response_rows = []
    for i in range(len(date_list)):
        duals = [duals_list[z][i] for z in range(len(duals_list))]
        response_rows.append(SSE.Row(duals=iter(duals)))

    return response_rows





