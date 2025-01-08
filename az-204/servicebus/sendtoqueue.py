from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv
import os
import json
load_dotenv()

# Substitua pelos seus valores
CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")

def send_message_to_queue(message_content):
    # Cria um cliente do Service Bus
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

    with servicebus_client:
        # Cria um sender para a fila
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)

        with sender:

            try:
                # Cria uma mensagem
                message = ServiceBusMessage(message_content)

                # Envia a mensagem para a fila
                sender.send_messages(message)
                dict_data = json.loads(message_content)
                print(f"Mensagem enviada: {dict_data['order_id']}")
            except Exception as e:
                print(f"Erro ao enviar mensagem: {e}")



if __name__ == "__main__":

    # get the current directory
    dir = os.path.dirname(__file__)
    file = os.path.join(dir, 'json', 'order2.json')
    with open(file) as f:
        json_data = json.load(f)
        message_content=json.dumps(json_data)

    # Envia a mensagem
    send_message_to_queue(message_content)
