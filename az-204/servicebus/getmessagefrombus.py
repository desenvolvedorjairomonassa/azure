import json
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import os
import json
from dotenv import load_dotenv

load_dotenv()


CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")


def process_message(message):
    # Decodifica a mensagem JSON
    json_data = json.loads(str(message))

    # Processa a mensagem JSON
    print(f"Mensagem recebida: {json_data['order_id']}")


def receive_messages():
    # Cria um cliente do Service Bus
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

    with servicebus_client:
        # Cria um receiver para a fila
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)

        with receiver:
            # Recebe mensagens da fila
            for msg in receiver:
                try:
                    # Processa a mensagem
                    process_message(msg)

                    # Marca a mensagem como completa, ou seja como lida
                    receiver.complete_message(msg)
                except Exception as e:
                    # Se ocorrer um erro, marca a mensagem como não processada
                    print(f"Erro ao processar a mensagem: {e}")
                    receiver.abandon_message(msg)

if __name__ == "__main__":
    # Recebe e processa mensagens da fila
    receive_messages()
