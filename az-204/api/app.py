from flask import Flask, request, jsonify
import hashlib
import uuid

app = Flask(__name__)

@app.route('/generate-hash', methods=['POST'])
def generate_hash():
    try:
        #coloque log
        print('Generating hash')
        
        # Pega o texto do body da requisição
        data = request.get_json()
        print('data:',data)
        text = data.get('text', '')
        
        # Se nenhum texto for fornecido, gera um UUID aleatório
        if not text:
            text = str(uuid.uuid4())
        
        # Gera diferentes tipos de hash
        hashes = {
            'md5': hashlib.md5(text.encode()).hexdigest(),
            'sha1': hashlib.sha1(text.encode()).hexdigest(),
            'sha256': hashlib.sha256(text.encode()).hexdigest(),
            'sha512': hashlib.sha512(text.encode()).hexdigest()
        }
        
        return jsonify({
            'original_text': text,
            'hashes': hashes
        }), 200
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 400

@app.route('/healthCheck', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'message': 'Hash generation service is running'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, port=3000)