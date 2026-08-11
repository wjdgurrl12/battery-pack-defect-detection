"""Streamlit 껍데기. 여기에 실제 화면을 채워 나간다."""

import os

import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:3000")

st.set_page_config(page_title="dockerimage", layout="wide")
st.title("dockerimage")

try:
    response = requests.get(f"{API_BASE_URL}/health", timeout=5)
    response.raise_for_status()
except requests.RequestException as e:
    st.error(f"API에 연결하지 못했습니다: {e}")
else:
    st.success(f"API 연결 정상: {response.json()}")