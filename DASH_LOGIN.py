import requests
from flask import Flask, render_template

def get_powerbi_embed_token(report_id, access_token):
    # формируем URL для получения токена встраивания отчета
    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}/GenerateToken"
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Bearer {access_token}'}

    # создаем параметры запроса
    body = {
        "accessLevel": "View",
        "allowSaveAs": "false",
        "enforceSingleCheck": "false"
    }

    # отправляем POST-запрос на API Power BI для получения токена Embed
    response = requests.post(url, headers=headers, json=body)
    token = response.json()['token']

    # возвращаем токен Embed
    return token


def embed_powerbi_report(report_id, access_token):
    # получаем токен Embed
    token = get_powerbi_embed_token(report_id, access_token)

    # формируем URL для встраивания отчета
    embed_url = f"https://app.powerbi.com/reportEmbed?reportId={report_id}&token={token}"

    # возвращаем код HTML для встраивания отчета
    return f'<iframe width="800" height="600" src="{embed_url}"></iframe>'


app = Flask(__name__)

@app.route('/')
def index():
    report_id = "YOUR_REPORT_ID"
    access_token = "YOUR_ACCESS_TOKEN"

    html_code = embed_powerbi_report(report_id, access_token)

    return render_template('index.html', html_code=html_code)

if __name__ == '__main__':
    app.run()