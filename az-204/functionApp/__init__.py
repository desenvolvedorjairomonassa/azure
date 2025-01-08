import logging
import azure.functions as func

def validate_cpf(cpf):
    # Remove caracteres não numéricos
    cpf = ''.join(filter(str.isdigit, cpf))

    # Verifica se o CPF tem 11 dígitos
    if len(cpf) != 11:
        return False

    # Verifica se todos os dígitos são iguais
    if cpf == cpf[0] * 11:
        return False

    # Calcula os dígitos verificadores
    def calculate_digit(cpf, factor):
        sum = 0
        for number in cpf[:9]:
            sum += int(number) * factor
            factor -= 1
        digit = 11 - (sum % 11)
        return digit if digit < 10 else 0

    first_digit = calculate_digit(cpf, 10)
    second_digit = calculate_digit(cpf[:9] + str(first_digit), 11)

    # Verifica se os dígitos verificadores estão corretos
    return cpf[-2:] == f"{first_digit}{second_digit}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    cpf = req.params.get('cpf')
    if not cpf:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            cpf = req_body.get('cpf')

    if cpf:
        is_valid = validate_cpf(cpf)
        if is_valid:
            return func.HttpResponse(f"CPF {cpf} é válido.", status_code=200)
        else:
            return func.HttpResponse(f"CPF {cpf} é inválido.", status_code=400)
    else:
        return func.HttpResponse(
             "Por favor, forneça um CPF na query string ou no corpo da requisição.",
             status_code=400
        )
