import requests


# 测试API连接
def test_api_connection():
    url = "http://222.171.219.26:20001/v1/chat/completions"
    headers = {
        "Authorization": "Bearer gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "qwen3-30b-a3b-instruct-2507",
        "messages": [{"role": "user", "content": "test"}],
        "temperature": 0.1
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        print(f"状态码: {response.status_code}")
        print(f"响应: {response.text[:200]}")
        return response.status_code == 200
    except Exception as e:
        print(f"连接失败: {e}")
        return False


test_api_connection()
