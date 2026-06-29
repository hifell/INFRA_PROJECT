import os
import json
import asyncio
import websockets
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "crypto_signals"
TOKENS = ["BTC", "ETH", "SOL", "XRP", "BNB"]

def create_kafka_producer():
    print(f"[*] Menghubungkan ke Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    return producer

async def binance_websocket():
    producer = create_kafka_producer()
    
    # Create combined stream URL
    streams = [f"{token.lower()}usdt@kline_1s" for token in TOKENS]
    stream_url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
    
    print(f"[*] Terhubung ke Binance WebSocket: {stream_url}")
    
    async for websocket in websockets.connect(stream_url):
        try:
            print("[+] WebSocket Terhubung. Menunggu data...")
            async for message in websocket:
                data = json.loads(message)
                
                # Payload combined stream ada di key "data"
                if "data" in data and "k" in data["data"]:
                    kline = data["data"]["k"]
                    
                    # Hanya proses jika candle sudah ditutup (is_closed = True)
                    # Ini menjamin kita dapat persis 1 baris final per detik
                    if kline["x"]:
                        token_name = kline["s"].replace("USDT", "")
                        # Convert timestamp ms ke string
                        dt = datetime.utcfromtimestamp(kline["t"] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        
                        payload = {
                            "token": token_name,
                            "Datetime": dt,
                            "Open": float(kline["o"]),
                            "High": float(kline["h"]),
                            "Low": float(kline["l"]),
                            "Close": float(kline["c"]),
                            "Volume": float(kline["v"])
                        }
                        
                        # Publish ke Kafka
                        producer.send(KAFKA_TOPIC, key=token_name.encode("utf-8"), value=payload)
                        print(f"[STREAM] {token_name} | {dt} | C: {payload['Close']}")
                        
        except websockets.ConnectionClosed:
            print("[!] WebSocket terputus. Mencoba menghubungkan kembali dalam 5 detik...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            print(f"[!] Error: {str(e)}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("[*] BINANCE WEBSOCKET PRODUCER: 1-SECOND CANDLES")
    print("=" * 60)
    asyncio.run(binance_websocket())
