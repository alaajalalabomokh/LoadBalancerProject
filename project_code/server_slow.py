import socket

import time



HOST = "0.0.0.0"

PORT = 5000



# זמן טיפול קבוע לכל סוג (שניות) - תשני לפי דרישות המורה

#SERVICE_TIMES = {

#    "M": 0.30,  # Music

#    "V": 0.60,  # Video

#    "P": 1.5,  # Photo

#}


#sim1  SERVICE_TIMES = {

#    "M": 0.15,  # Music

#    "V": 0.40,  # Video

#    "P": 0.08,  # Photo

#}


#sim2
#SERVICE_TIMES = {
#    "M": 0.30,
#    "V": 0.80,
#    "P": 0.20
#}


#sim3
SERVICE_TIMES = {
    "M": 0.50,
    "V": 1.20,
    "P": 0.30
}



s = socket.socket()

s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Prevents "address already in use"

s.bind((HOST, PORT))

s.listen(500)



request_count = 0

print(f"Server is listening on port {PORT}...")



while True:

    conn, addr = s.accept()
    request_count += 1

    try:
        data = conn.recv(4096)
        start_time=time.time()
        if not data:
            conn.close()
            continue

        line = data.decode(errors="ignore").strip()
        parts = line.split()
        req_type = parts[0].upper() if len(parts) >= 1 else "?"
        req_id = parts[1] if len(parts) >= 2 else str(request_count)
        service = SERVICE_TIMES.get(req_type, 0.10)  # default אם לא מוכר
        print(f"Request #{request_count} from {addr[0]}: type={req_type} id={req_id} service={service}s")
        #while(start_time+service>time.time()):
        #    pass
        time.sleep(service)

        #time.sleep(service)

        resp = f"OK {req_type} {req_id} service={service}\n"

        conn.sendall(resp.encode())



    except Exception as e:

        try:

            conn.sendall(b"ERR\n")

        except Exception:

            pass

    finally:

        conn.close()



