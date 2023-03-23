import pandas as pd
import numpy as np
from lib import ServerSideExtension_pb2 as SSE

def media_movel(request, context):
    
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

    df_meses = pd.DataFrame()
    for i in range(1,13):
        df['ano'] = df.index.year
        df['mes'] = df.index.month
        mes_selec = df[df['mes'] == i]
        mes_selec['media'] = round(mes_selec['quantidade'].rolling(3).mean().shift())
        df_meses = pd.concat([df_meses, mes_selec])

    df_meses.drop(['ano', 'mes'], axis=1, inplace=True)
    df_meses = df_meses.sort_index()
    df_meses = df_meses.fillna(0)

    df_meses = df_meses.reset_index()
    df_meses.columns = ['data','quantidade','media_movel']
    df_meses['data'] = df_meses['data'].astype(str)
    df_meses['quantidade'] = df_meses['quantidade'].astype(str)
    df_meses['media_movel'] = df_meses['media_movel'].astype(int)
    df_meses['media_movel'] = df_meses['media_movel'].astype(str)

    data_list = df_meses['data'].values.tolist()
    quantidade_list = df_meses['quantidade'].values.tolist()
    media_list = df_meses['media_movel'].values.tolist()
    
    duals_list = []
    duals_list.append([SSE.Dual(strData=d) for d in data_list])
    duals_list.append([SSE.Dual(strData=d) for d in quantidade_list])
    duals_list.append([SSE.Dual(strData=d) for d in media_list])

    response_rows = []
    for i in range(len(data_list)):
        duals = [duals_list[z][i] for z in range(len(duals_list))]
        response_rows.append(SSE.Row(duals=iter(duals)))

    return response_rows