# MQTT

# Import dos pacotes
import pandas as pd
import time
import paho.mqtt.client as mqtt

# Criacao de um DataFrame para coleta de dados
df_clients = pd.DataFrame(columns = ['dt', 'clients'])


# Funcao ativada (callback) quando o client recebe a confirmacao de conexao
# com o broker (CONNACK).
def on_connect(client, userdata, flags, rc):
    print("Conectado com o resultado código " + str(rc))

    # Inscricao (subscribing) em um topico do broker.
    # Utilizar o metodo subscribe na funcao on_connect permite que, caso a
    # conexao seja perdida e reestabelecida a incricao e realizada novamente
    client.subscribe("$SYS/broker/clients/active")


# Funcao ativada (callback) quando uma mensagem publicada e recebida pelo
# cliente
def on_message(client, userdata, msg):
    
    global df_clients
    
    print(msg)
    print(msg.topic+" "+str(msg.payload))
    print(int(msg.payload))
    print(time.ctime())
    print(time.strftime("%Y%M%d %H:%M:%S"))
    
    df_clients = df_clients.append({'dt' : time.strftime("%Y%M%d %H:%M:%S"), 
                                    'clients' : int(msg.payload)},
                                    ignore_index=True)
    
    print(df_clients)
     

# Cricacao do objeto client do mqtt e definicao das funcoes de callback
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Conexao com o broker
client.connect("mqtt.eclipseprojects.io", 1883)

# Metodo que gerencia a conexao com o broker: processa o trafego de rede,
# dispara as chamadas das funcoes de callback e trata dos processos de reconexao
client.loop_forever()


-------- End


# Import dos pacotes
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish


# Funcao ativada (callback) quando o client recebe a confirmacao de conexao
# com o broker (CONNACK).
def on_connect(client, userdata, flags, rc):

    client.subscribe("$SYS/broker/clients/active")


# Funcao ativada (callback) quando uma mensagem publicada e recebida pelo
# cliente
def on_message(client, userdata, msg):
    
    dado = int(msg.payload) + 1000
    print("Enviando o valor:", dado)
    publish.single("paho/test/mack", dado, hostname="mqtt.eclipseprojects.io")

     

# Cricacao do objeto client do mqtt e definicao das funcoes de callback
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Conexao com o broker
client.connect("mqtt.eclipseprojects.io")

# Metodo que gerencia a conexao com o broker: processa o trafego de rede,
# dispara as chamadas das funcoes de callback e trata dos processos de reconexao
client.loop_forever()
