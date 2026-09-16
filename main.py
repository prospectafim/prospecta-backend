"""
PROSPECTA FIM — Backend API
Núcleo de Finanças Insper

Deploy: Railway
Stack: FastAPI + PostgreSQL + APScheduler
Roda o batimento de cotas todo dia às 18h (horário de Brasília)
e expõe uma API REST que o Hub HTML consome.
"""

import os
import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── ANBIMA API ──
ANBIMA_CLIENT_ID     = os.environ.get("ANBIMA_CLIENT_ID", "x8eeOKFhWwvF")
ANBIMA_CLIENT_SECRET = os.environ.get("ANBIMA_CLIENT_SECRET", "0AZiRk0E4rON")
ANBIMA_TOKEN_URL     = "https://api.anbima.com.br/oauth/access-token"
ANBIMA_API_BASE      = "https://api.anbima.com.br/feed/precos-indices/v1"
_anbima_token_cache: dict = {"token": None, "expires_at": 0}

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PL_INICIAL   = 10_000_000.0
DATA_T0      = date(2025, 4, 30)
COTA_T0      = 1.0

# Tickers Yahoo Finance → nome interno
YAHOO_TICKERS = {
    "IVV":    "IVV",
    "IAU":    "IAU",
    "STIP":   "STIP",
    "URNM":   "URNM",
    "REMX":   "REMX",
    "CPER":   "CPER",
    "CORN":   "CORN",
    "CANE":   "CANE",
    "BTC-USD":"Bitcoin",
    "BRL=X":  "USDBRL",
    "EURUSD=X":"EURUSD",
    "BOVA11.SA":"BOVA11",
    "UTLL11.SA":"UTLL11",
    "RAIL3.SA": "RAIL3",
    "SMAL11.SA":"SMAL11",
    "CURY3.SA": "CURY3",
}

# Carteiras históricas: (data, {ativo: peso})
CARTEIRAS = [
    (date(2025, 4, 30), {
        "LFT 2031":0.14,"NTN-B 2029":0.27,"IAU":0.12,"IVV":0.08
    }),
    (date(2025, 8, 29), {
        "LTN 2032":0.46,"IVV":0.304,"IAU":0.109
    }),
    (date(2025, 9, 30), {
        "LTN 2032":0.45,"BOVA11":0.135,"IAU":0.103,"IVV":0.06
    }),
    (date(2025, 10, 31), {
        "LTN 2032":0.47,"BOVA11":0.115,"IAU":0.075,"IVV":0.06,
        "URNM":0.06,"REMX":0.05,"Bitcoin":0.03,"CPER":0.02
    }),
    (date(2026, 1, 30), {
        "LTN 2032":0.50,"BOVA11":0.115,"IAU":0.075,"IVV":0.06,
        "URNM":0.06,"REMX":0.05,"Bitcoin":0.03,"CPER":0.02
    }),
    (date(2026, 3, 20), {
        "LTN 2032":0.25,"NTN-B 2040":0.13,"LFT 2031":0.15,
        "BOVA11":0.08,"IAU":0.08,"URNM":0.07,"IVV":0.06,
        "REMX":0.05,"STIP":0.03,"Bitcoin":0.03,"RAIL3":0.02,"CPER":0.02
    }),
    (date(2026, 4, 24), {
        "NTN-B 2040":0.185,"LFT 2031":0.15,"LTN 2032":0.13,
        "IVV":0.10,"STIP":0.07,"Swedish Gov Bond":0.05,"IAU":0.05,
        "Siemens Bond":0.04,"BOVA11":0.03,"UTLL11":0.03,
        "CPER":0.025,"URNM":0.02,"REMX":0.02,"RAIL3":0.02,
        "CORN":0.015,"CANE":0.01,"Bitcoin":0.01,
    }),
    (date(2026, 5, 29), {
        "LFT 2031":0.24,"NTN-B 2029":0.195,"NTN-B 2035":0.145,
        "STIP":0.07,"IAU":0.05,"Swedish Gov Bond":0.05,
        "Siemens Bond":0.04,"IVV":0.03,"BOVA11":0.03,"UTLL11":0.03,
        "CPER":0.025,"URNM":0.02,"REMX":0.02,"RAIL3":0.02,
        "CORN":0.015,"CANE":0.01,"Bitcoin":0.01,
    }),
    (date(2026, 8, 6), {
        "LFT 2031":0.240,"NTN-B 2029":0.195,"NTN-B 2035":0.145,
        "STIP":0.070,"IAU":0.050,"IVV":0.040,
        "BOVA11":0.050,"UTLL11":0.040,"RAIL3":0.020,"CURY3":0.030,
        "CPER":0.025,"CORN":0.025,"CANE":0.020,
        "URNM":0.020,"REMX":0.020,"Bitcoin":0.010,
    }),
    (date(2026, 9, 3), {
        "LFT 2031":0.240,"NTN-B 2029":0.195,"NTN-B 2035":0.145,
        "STIP":0.070,"IAU":0.050,"IVV":0.040,
        "BOVA11":0.050,"UTLL11":0.040,"RAIL3":0.020,"CURY3":0.030,
        "CPER":0.025,"CORN":0.025,
        "URNM":0.020,"REMX":0.020,"Bitcoin":0.010,
    }),
    (date(2026, 9, 11), {
        "LFT 2031":0.240,"NTN-B 2029":0.195,"NTN-B 2035":0.145,
        "STIP":0.070,"IAU":0.050,"IVV":0.040,
        "BOVA11":0.050,"UTLL11":0.040,"RAIL3":0.020,"CURY3":0.030,
        "CPER":0.025,"CANE":0.020,
        "URNM":0.020,"REMX":0.020,"Bitcoin":0.010,
    }),
]

# Futuros/derivativos
FUTUROS = {
    "EUR/BRL":  {"entrada": date(2026, 4, 24), "long": False, "notional": 0.08,  "preco_ref": "EURUSD"},
    "MXN/CAD":  {"entrada": date(2026, 4, 27), "long": True,  "notional": 0.03,  "preco_ref": "MXNCAD"},
}

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    """Cria as tabelas se não existirem."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cotas_diarias (
            id          SERIAL PRIMARY KEY,
            data        DATE UNIQUE NOT NULL,
            cota        NUMERIC(12,6) NOT NULL,
            pl          NUMERIC(18,2),
            retorno_dia NUMERIC(12,8),
            created_at  TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS precos_ativos (
            id         SERIAL PRIMARY KEY,
            data       DATE NOT NULL,
            ativo      VARCHAR(64) NOT NULL,
            preco      NUMERIC(18,6) NOT NULL,
            fonte      VARCHAR(32),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(data, ativo)
        );

        CREATE TABLE IF NOT EXISTS cdi_mensal (
            mes        VARCHAR(7) PRIMARY KEY,  -- formato YYYY-MM
            taxa       NUMERIC(8,6) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS precos_manuais (
            ativo      VARCHAR(64) PRIMARY KEY,
            preco      NUMERIC(18,6) NOT NULL,
            data_ref   DATE NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS pesos_meta (
            id         SERIAL PRIMARY KEY,
            data_ini   DATE NOT NULL,
            ativo      VARCHAR(64) NOT NULL,
            peso       NUMERIC(8,6) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(data_ini, ativo)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized")

    # Inserir CDI histórico se tabela estiver vazia
    _seed_cdi()

def _seed_cdi():
    """Insere o CDI histórico conhecido."""
    cdi_historico = {
        "2025-05": 0.01140, "2025-06": 0.01100, "2025-07": 0.01280,
        "2025-08": 0.01160, "2025-09": 0.01220, "2025-10": 0.01280,
        "2025-11": 0.01050, "2025-12": 0.01220, "2026-01": 0.01160,
        "2026-02": 0.01000, "2026-03": 0.01210, "2026-04": 0.01090,
        "2026-05": 0.01070,
    }
    conn = get_conn()
    cur = conn.cursor()
    for mes, taxa in cdi_historico.items():
        cur.execute("""
            INSERT INTO cdi_mensal (mes, taxa)
            VALUES (%s, %s)
            ON CONFLICT (mes) DO NOTHING
        """, (mes, taxa))
    conn.commit()
    cur.close()
    conn.close()

# ─────────────────────────────────────────────
# BUSCA DE PREÇOS
# ─────────────────────────────────────────────
def fetch_yahoo_single(ticker: str) -> Optional[float]:
    """Busca preço de um ticker via Yahoo Finance API v8 (sem yfinance)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning(f"Yahoo v8 {ticker}: HTTP {r.status_code}")
            return None
        data_json = r.json()
        result = data_json.get("chart", {}).get("result", [])
        if not result:
            return None
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        return float(closes[-1]) if closes else None
    except Exception as e:
        logger.warning(f"Yahoo v8 {ticker}: {e}")
        return None

def fetch_yahoo_v2(ticker: str) -> Optional[float]:
    """Busca preço via Yahoo Finance v7."""
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        data_json = r.json()
        result = data_json.get("chart", {}).get("result", [])
        if not result:
            return None
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        return float(closes[-1]) if closes else None
    except Exception as e:
        logger.warning(f"Yahoo v2 {ticker}: {e}")
        return None

def fetch_yahoo(tickers: list, data: date) -> dict:
    """Busca preços de fechamento do Yahoo Finance — tenta múltiplos métodos."""
    precos = {}

    # Método 1: yfinance library
    try:
        start = data - timedelta(days=5)
        end   = data + timedelta(days=1)
        raw = yf.download(
            tickers, start=start.isoformat(), end=end.isoformat(),
            auto_adjust=True, progress=False, threads=False
        )
        if not raw.empty:
            close = raw["Close"] if len(tickers) > 1 else raw[["Close"]]
            if len(tickers) > 1:
                close.columns = tickers
            for t in tickers:
                if t in close.columns:
                    series = close[t].dropna()
                    if not series.empty:
                        precos[t] = float(series.iloc[-1])
            if precos:
                logger.info(f"  yfinance: {len(precos)}/{len(tickers)} preços")
                return precos
    except Exception as e:
        logger.warning(f"yfinance failed: {e}")

    # Método 2: Yahoo Finance API v8 diretamente
    logger.info("  Tentando Yahoo Finance API v8...")
    for t in tickers:
        if t not in precos:
            p = fetch_yahoo_single(t)
            if p is None:
                p = fetch_yahoo_v2(t)
            if p is not None:
                precos[t] = p
            else:
                logger.warning(f"  Sem preço para {t}")

    logger.info(f"  Yahoo API: {len(precos)}/{len(tickers)} preços obtidos")
    return precos

def get_anbima_token() -> Optional[str]:
    """Obtém token OAuth2 da ANBIMA com cache (Basic Auth)."""
    import time, base64
    now = time.time()
    if _anbima_token_cache["token"] and now < _anbima_token_cache["expires_at"] - 60:
        return _anbima_token_cache["token"]
    try:
        credentials = base64.b64encode(
            f"{ANBIMA_CLIENT_ID}:{ANBIMA_CLIENT_SECRET}".encode()
        ).decode()
        r = requests.post(
            ANBIMA_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=15,
        )
        if r.status_code not in (200, 201):
            logger.error(f"ANBIMA token error: {r.status_code} {r.text[:200]}")
            return None
        data = r.json()
        _anbima_token_cache["token"] = data["access_token"]
        _anbima_token_cache["expires_at"] = now + int(data.get("expires_in", 3600))
        logger.info("ANBIMA token obtido com sucesso")
        return _anbima_token_cache["token"]
    except Exception as e:
        logger.error(f"ANBIMA token exception: {e}")
        return None


def fetch_anbima_pu(data_ref: date) -> dict:
    """
    Calcula PUs dos Tesouros via BCB API (Selic over diária).
    LFT: PU_hoje = PU_ontem * (1 + taxa_selic_dia/100)
    NTN-B: idem como proxy conservador
    """
    precos = {}

    try:
        # ── Busca taxa Selic over do dia via BCB ──
        # Serie 11 = Selic over diária (% ao dia)
        data_br = data_ref.strftime("%d/%m/%Y")
        url_selic = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
            f"?formato=json&dataInicial={data_br}&dataFinal={data_br}"
        )
        r_selic = requests.get(url_selic, timeout=10)

        taxa_selic = None
        if r_selic.status_code == 200:
            selic_data = r_selic.json()
            if selic_data and len(selic_data) > 0:
                taxa_selic = float(selic_data[0]['valor'])
                logger.info(f"BCB Selic {data_ref}: {taxa_selic}% ao dia")

        # Se nao encontrou a taxa do dia (fim de semana, feriado),
        # busca a ultima taxa disponivel
        if taxa_selic is None:
            r_selic2 = requests.get(
                "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/ultimos/1?formato=json",
                timeout=10
            )
            if r_selic2.status_code == 200:
                dados = r_selic2.json()
                if dados:
                    taxa_selic = float(dados[0]['valor'])
                    logger.info(f"BCB Selic fallback (ultimo): {taxa_selic}%")

        if taxa_selic is None:
            # Estimativa conservadora: Selic 14% a.a. / 252 dias
            taxa_selic = 14.0 / 252 / 100 * 100  # ~0.0556% ao dia
            logger.warning(f"BCB indisponivel — usando taxa estimada {taxa_selic}%")

        # ── Busca PU anterior do banco ──
        conn = get_conn()
        cur = conn.cursor()

        for titulo, nome in [('LFT 2031', 'LFT 2031'),
                              ('NTN-B 2029', 'NTN-B 2029'),
                              ('NTN-B 2035', 'NTN-B 2035')]:
            cur.execute("""
                SELECT preco, data FROM precos_ativos
                WHERE ativo = %s AND data < %s
                AND preco > 1000
                ORDER BY data DESC LIMIT 1
            """, (nome, data_ref))
            row = cur.fetchone()

            if row:
                pu_ant = float(row['preco'])
                data_ant = row['data']
                # Calcula quantos dias uteis passaram para compor corretamente
                # (simplificado: aplica 1 vez a taxa — ok para dias consecutivos)
                pu_novo = round(pu_ant * (1 + taxa_selic / 100), 6)
                precos[nome] = pu_novo
                logger.info(f"{nome}: {pu_ant} ({data_ant}) → {pu_novo} (+{taxa_selic}%)")
            else:
                logger.warning(f"{nome}: sem PU anterior no banco para {data_ref}")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"fetch_anbima_pu error: {e}")

    return precos


def fetch_tesouro(data: date) -> dict:
    """
    Calcula PUs dos Tesouros via BCB (Selic over diária).
    Não usa mais a API do Tesouro Direto — ela retorna valores desatualizados.
    """
    return fetch_anbima_pu(data)


@app.put("/api/cdi")
def upsert_cdi(mes: str, taxa: float):
    """
    Atualiza o CDI de um mês. mes = 'YYYY-MM', taxa = decimal (ex: 0.0107)
    Chamado automaticamente pelo scheduler ou manualmente pelo Hub.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cdi_mensal (mes, taxa)
        VALUES (%s, %s)
        ON CONFLICT (mes) DO UPDATE SET taxa = EXCLUDED.taxa
    """, (mes, taxa))
    conn.commit()
    cur.close()
    conn.close()

    # Tenta buscar do BCB automaticamente
    taxa_bcb = fetch_bcb_cdi(mes)
    if taxa_bcb:
        cur2 = conn.cursor() if not conn.closed else get_conn().cursor()
        conn2 = get_conn()
        cur2 = conn2.cursor()
        cur2.execute("""
            INSERT INTO cdi_mensal (mes, taxa)
            VALUES (%s, %s)
            ON CONFLICT (mes) DO UPDATE SET taxa = EXCLUDED.taxa
        """, (mes, taxa_bcb))
        conn2.commit()
        cur2.close()
        conn2.close()
        return {"ok": True, "mes": mes, "taxa": taxa_bcb, "fonte": "BCB"}

    return {"ok": True, "mes": mes, "taxa": taxa, "fonte": "manual"}

@app.get("/api/cdi")
def get_cdi():
    """Retorna todos os CDIs registrados."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT mes, taxa FROM cdi_mensal ORDER BY mes")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/carteira")
def get_carteira_atual():
    """Retorna a carteira vigente com pesos."""
    hoje = date.today()
    carteira = get_carteira_vigente(hoje)
    return {
        "data": str(hoje),
        "posicoes": [{"ativo": k, "peso": v} for k, v in carteira.items()],
        "derivativos": [
            {
                "nome": k,
                "long": v["long"],
                "notional": v["notional"],
                "entrada": str(v["entrada"])
            }
            for k, v in FUTUROS.items()
            if v["entrada"] <= hoje
        ]
    }


@app.post("/api/carga-historica")
def carga_historica(payload: dict):
    """
    Endpoint de carga histórica — usado uma única vez para popular o banco
    com o histórico de cotas desde o início do fundo.
    Payload: {data, cota, pl, retorno}
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO cotas_diarias (data, cota, pl, retorno_dia)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data) DO UPDATE
            SET cota = EXCLUDED.cota,
                pl = EXCLUDED.pl,
                retorno_dia = EXCLUDED.retorno_dia
        """, (
            payload["data"],
            payload["cota"],
            payload.get("pl"),
            payload.get("retorno")
        ))
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "data": payload["data"]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/cota/{data_str}")
def delete_cota(data_str: str):
    """Remove uma cota específica do banco (usado para limpar dias zerados)."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM cotas_diarias WHERE data = %s", (data_str,))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "data": data_str, "deleted": deleted}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/carga-precos")
def carga_precos(payload: dict):
    """
    Carrega preços históricos diretamente na tabela precos_ativos.
    Usado para popular dados históricos de Tesouros e outros ativos.
    Payload: {data, ativo, preco, fonte}
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO precos_ativos (data, ativo, preco, fonte)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data, ativo) DO UPDATE
            SET preco = EXCLUDED.preco, fonte = EXCLUDED.fonte
        """, (
            payload["data"],
            payload["ativo"],
            payload["preco"],
            payload.get("fonte", "manual")
        ))
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "data": payload["data"], "ativo": payload["ativo"]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/preco/{ativo}/{data_str}")
def delete_preco(ativo: str, data_str: str):
    """Remove um preço específico de um ativo em uma data."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM precos_ativos WHERE ativo = %s AND data = %s",
            (ativo, data_str)
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "ativo": ativo, "data": data_str, "deleted": deleted}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/atribuicao")
def get_atribuicao(mes: str):
    """
    Retorna atribuicao de performance por ativo num mes.
    mes = 'YYYY-MM'
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        ano, m = int(mes[:4]), int(mes[5:7])
        mes_inicio = f"{ano}-{m:02d}-01"
        if m == 12:
            mes_fim_dt = date(ano+1, 1, 1)
        else:
            mes_fim_dt = date(ano, m+1, 1)
        mes_fim_str = str(mes_fim_dt)

        # Cotas do mes
        cur.execute("""
            SELECT data, cota, retorno_dia FROM cotas_diarias
            WHERE data >= %s AND data < %s
            ORDER BY data
        """, (mes_inicio, mes_fim_str))
        cotas_mes = cur.fetchall()

        if not cotas_mes:
            raise HTTPException(404, "Sem cotas para este mes")

        data_inicio = str(cotas_mes[0]['data'])
        data_fim    = str(cotas_mes[-1]['data'])

        # Cota do ultimo dia do mes anterior (para retorno do mes)
        cur.execute("""
            SELECT cota FROM cotas_diarias
            WHERE data < %s ORDER BY data DESC LIMIT 1
        """, (mes_inicio,))
        row_ant = cur.fetchone()
        cota_inicio = float(row_ant['cota']) if row_ant else 1.0
        cota_fim    = float(cotas_mes[-1]['cota'])
        ret_total   = cota_fim / cota_inicio - 1 if cota_inicio > 0 else 0

        # Carteira vigente no fim do mes (mais representativa)
        carteira = get_carteira_vigente(date.fromisoformat(data_fim))

        ativos_usd = {
            "IVV","IAU","STIP","URNM","REMX","CPER","CORN","CANE",
            "Bitcoin","Swedish Gov Bond","Siemens Bond","CURY3_USD"
        }

        resultado = []
        for ativo, peso in carteira.items():
            # Preco no ultimo dia do mes anterior (referencia de inicio)
            cur.execute("""
                SELECT preco, data FROM precos_ativos
                WHERE ativo = %s AND data < %s
                ORDER BY data DESC LIMIT 1
            """, (ativo, mes_inicio))
            row_i = cur.fetchone()

            # Ativo entrou no mes (sem preco anterior): usa primeiro preco do mes
            if not row_i:
                cur.execute("""
                    SELECT preco, data FROM precos_ativos
                    WHERE ativo = %s AND data >= %s AND data <= %s
                    ORDER BY data ASC LIMIT 1
                """, (ativo, mes_inicio, data_fim))
                row_i = cur.fetchone()

            # Preco no fim do mes
            cur.execute("""
                SELECT preco, data FROM precos_ativos
                WHERE ativo = %s AND data <= %s
                ORDER BY data DESC LIMIT 1
            """, (ativo, data_fim))
            row_f = cur.fetchone()

            if not row_i or not row_f:
                resultado.append({
                    "ativo": ativo, "peso": round(peso, 4),
                    "var_mes": None, "contribuicao": None, "sem_dados": True
                })
                continue

            pi = float(row_i['preco'])
            pf = float(row_f['preco'])

            if pi <= 0:
                resultado.append({
                    "ativo": ativo, "peso": round(peso, 4),
                    "var_mes": None, "contribuicao": None, "sem_dados": True
                })
                continue

            # ── Valida PUs de Tesouros ──
            # Se pf == pi (congelado) ou pf == 19412.15 (valor antigo),
            # recalcula usando Selic acumulada do período
            TESOUROS_SET = {"LFT 2031", "NTN-B 2029", "NTN-B 2035", "LTN 2032"}
            # Tesouros nunca podem ter PU menor que o do mes anterior
            # Se pf < pi, o PU esta errado (congelado ou valor historico)
            if ativo in TESOUROS_SET and pf <= pi * 1.0001:
                try:
                    # Busca Selic acumulada do período via BCB
                    from datetime import datetime
                    d_ini = date.fromisoformat(data_inicio)
                    d_fim = date.fromisoformat(data_fim)
                    d_ini_br = d_ini.strftime("%d/%m/%Y")
                    d_fim_br = d_fim.strftime("%d/%m/%Y")
                    r_s = requests.get(
                        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
                        f"?formato=json&dataInicial={d_ini_br}&dataFinal={d_fim_br}",
                        timeout=8
                    )
                    if r_s.status_code == 200 and r_s.json():
                        fator = 1.0
                        for item in r_s.json():
                            fator *= (1 + float(item["valor"]) / 100)
                        # Busca PU correto antes do inicio do mes
                        cur.execute("""
                            SELECT preco FROM precos_ativos
                            WHERE ativo = %s AND data < %s AND preco > 1000
                            ORDER BY data DESC LIMIT 1
                        """, (ativo, mes_inicio))
                        row_pi_corr = cur.fetchone()
                        if row_pi_corr:
                            pi = float(row_pi_corr["preco"])
                            pf = round(pi * fator, 6)
                            logger.info(f"Atrib {ativo}: PU recalculado via Selic — pi={pi} pf={pf} fator={fator:.6f}")
                except Exception as e_selic:
                    logger.warning(f"Atrib Selic recalc {ativo}: {e_selic}")

            if ativo in ativos_usd:
                # Ajusta pelo cambio USD/BRL
                cur.execute("""
                    SELECT preco FROM precos_ativos
                    WHERE ativo = 'USDBRL' AND data < %s
                    ORDER BY data DESC LIMIT 1
                """, (mes_inicio,))
                fx_i = cur.fetchone()
                cur.execute("""
                    SELECT preco FROM precos_ativos
                    WHERE ativo = 'USDBRL' AND data <= %s
                    ORDER BY data DESC LIMIT 1
                """, (data_fim,))
                fx_f = cur.fetchone()

                if fx_i and fx_f and float(fx_i['preco']) > 0:
                    val_i = pi * float(fx_i['preco'])
                    val_f = pf * float(fx_f['preco'])
                    var = val_f / val_i - 1
                else:
                    var = pf / pi - 1
            else:
                # Tesouros (PU em BRL), acoes BR, etc — direto
                var = pf / pi - 1

            contribuicao = peso * var

            resultado.append({
                "ativo": ativo,
                "peso": round(peso, 4),
                "preco_inicio": round(pi, 4),
                "preco_fim": round(pf, 4),
                "var_mes": round(var, 6),
                "contribuicao": round(contribuicao, 6),
                "sem_dados": False
            })

        # Ordena: piores primeiro
        resultado.sort(key=lambda x: (x.get('contribuicao') or 0))

        cur.close()
        conn.close()

        return {
            "mes": mes,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "retorno_mes": round(ret_total, 6),
            "ativos": resultado
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/pesos-reais")
def get_pesos_reais():
    """
    Retorna pesos meta e pesos reais de cada ativo com desvio calculado.
    Alerta quando desvio >= 3 pontos percentuais.
    """
    try:
        hoje = date.today()
        pesos_meta   = get_pesos_meta(hoje)
        pesos_reais  = calcular_pesos_reais(hoje)

        resultado = []
        for ativo, peso_meta in pesos_meta.items():
            peso_real = pesos_reais.get(ativo, peso_meta)
            desvio    = peso_real - peso_meta
            alerta    = abs(desvio) >= 0.03  # 3 pontos percentuais

            resultado.append({
                "ativo":      ativo,
                "peso_meta":  round(peso_meta, 4),
                "peso_real":  round(peso_real, 4),
                "desvio":     round(desvio, 4),
                "alerta":     alerta,
            })

        # Ordena por desvio absoluto (maiores desvios primeiro)
        resultado.sort(key=lambda x: abs(x["desvio"]), reverse=True)

        alertas = [r for r in resultado if r["alerta"]]

        return {
            "data":    str(hoje),
            "ativos":  resultado,
            "n_alertas": len(alertas),
            "alertas": alertas,
        }
    except Exception as e:
        raise HTTPException(500, str(e))




@app.post("/api/preco-manual")
def inserir_preco_manual(payload: dict):
    """Insere ou atualiza preco de um ativo no banco."""
    try:
        data_str = payload.get("data")
        ativo    = payload.get("ativo")
        preco    = payload.get("preco")
        fonte    = payload.get("fonte", "manual")
        if not data_str or not ativo or preco is None:
            raise HTTPException(400, "data, ativo e preco sao obrigatorios")
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO precos_ativos (data, ativo, preco, fonte)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data, ativo) DO UPDATE SET preco = EXCLUDED.preco, fonte = EXCLUDED.fonte
        """, (data_str, ativo, float(preco), fonte))
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "data": data_str, "ativo": ativo, "preco": preco}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/batimento-historico")
def batimento_historico(payload: dict):
    """Recalcula a cota de um dia especifico usando precos ja salvos no banco."""
    try:
        data_str = payload.get("data")
        if not data_str:
            raise HTTPException(400, "data obrigatoria")
        data_obj = date.fromisoformat(data_str)
        conn = get_conn()
        cur  = conn.cursor()
        carteira = get_carteira_vigente(data_obj)
        cur.execute("""
            SELECT cota FROM cotas_diarias
            WHERE data < %s ORDER BY data DESC LIMIT 1
        """, (data_obj,))
        row_ant = cur.fetchone()
        cota_anterior = float(row_ant['cota']) if row_ant else 1.0
        ativos_usd = {
            "IVV","IAU","STIP","URNM","REMX","CPER","CORN","CANE",
            "Bitcoin","Swedish Gov Bond","Siemens Bond","CURY3"
        }
        cur.execute("""
            SELECT preco FROM precos_ativos
            WHERE ativo = 'USDBRL' AND data <= %s ORDER BY data DESC LIMIT 1
        """, (data_obj,))
        fx_h = cur.fetchone()
        usdbrl_h = float(fx_h['preco']) if fx_h else None
        cur.execute("""
            SELECT preco FROM precos_ativos
            WHERE ativo = 'USDBRL' AND data < %s ORDER BY data DESC LIMIT 1
        """, (data_obj,))
        fx_a = cur.fetchone()
        usdbrl_a = float(fx_a['preco']) if fx_a else None
        retorno_total = 0.0
        for ativo, peso in carteira.items():
            cur.execute("""
                SELECT preco FROM precos_ativos
                WHERE ativo = %s AND data <= %s ORDER BY data DESC LIMIT 1
            """, (ativo, data_obj))
            ph_row = cur.fetchone()
            cur.execute("""
                SELECT preco FROM precos_ativos
                WHERE ativo = %s AND data < %s ORDER BY data DESC LIMIT 1
            """, (ativo, data_obj))
            pa_row = cur.fetchone()
            if not ph_row or not pa_row:
                continue
            ph = float(ph_row['preco'])
            pa = float(pa_row['preco'])
            if pa <= 0:
                continue
            if ativo in ativos_usd and usdbrl_h and usdbrl_a and usdbrl_a > 0:
                ret = (ph * usdbrl_h) / (pa * usdbrl_a) - 1
            else:
                ret = ph / pa - 1
            retorno_total += peso * ret
        nova_cota = round(cota_anterior * (1 + retorno_total), 10)
        pl_novo   = round(nova_cota * 10_000_000, 2)
        cur.execute("""
            INSERT INTO cotas_diarias (data, cota, retorno_dia, pl)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data) DO UPDATE
            SET cota=EXCLUDED.cota, retorno_dia=EXCLUDED.retorno_dia, pl=EXCLUDED.pl
        """, (data_obj, nova_cota, retorno_total, pl_novo))
        conn.commit()
        cur.close()
        conn.close()
        return {"data": data_str, "cota": nova_cota, "retorno": retorno_total, "pl": pl_novo}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/ping")
def ping():
    """Keep-alive para Render free tier."""
    return {"pong": True}



@app.post("/api/cota-manual")
def inserir_cota_manual(payload: dict):
    """Insere ou sobrescreve uma cota manualmente (ex: ajuste de P&L de derivativo)."""
    try:
        data_str  = payload.get("data")
        cota      = payload.get("cota")
        retorno   = payload.get("retorno_dia")
        pl        = payload.get("pl")
        if not data_str or cota is None:
            raise HTTPException(400, "data e cota sao obrigatorios")
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO cotas_diarias (data, cota, retorno_dia, pl)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data) DO UPDATE
            SET cota=EXCLUDED.cota,
                retorno_dia=EXCLUDED.retorno_dia,
                pl=EXCLUDED.pl
        """, (data_str, float(cota), float(retorno) if retorno else None,
              float(pl) if pl else None))
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "data": data_str, "cota": cota}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/status")
def get_status():
    """Retorna status do sistema e última atualização."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT data, cota FROM cotas_diarias ORDER BY data DESC LIMIT 1")
    ultima = cur.fetchone()
    cur.execute("SELECT COUNT(*) as total FROM cotas_diarias")
    total = cur.fetchone()
    cur.close()
    conn.close()
    return {
        "status": "ok",
        "ultima_cota": dict(ultima) if ultima else None,
        "total_dias": int(total["total"]) if total else 0,
        "timestamp": datetime.now().isoformat(),
    }
