import os
import time
import zipfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

from src.io.get_files_dict import main as get_files_dict
from src.io.utils import create_folder

# Configurações
CHUNK_SIZE = 1024 * 8  # 8KB
MAX_RETRIES = 5
BACKOFF_FACTOR = 0.5
TIMEOUT = 60  # segundos

dict_status = {}

def main():  # pragma: no cover
    """
    Função principal que gerencia o download dos arquivos.
    Verifica quais arquivos precisam ser baixados e os processa sequencialmente.
    """
    dict_files_dict = get_files_dict()
    create_folder(dict_files_dict['folder_ref_date_save_zip'])
    started_at = time.time()
    download_tasks = []

    # Identificar arquivos para download
    for tbl in dict_files_dict.keys():
        if isinstance(dict_files_dict[tbl], str):
            continue
            
        for file, entry in dict_files_dict[tbl].items():
            path = entry['path_save_file']
            if valid_file_exists(path):
                print(f"'{path:60}' - [OK] Already downloaded")
            else:
                print(f"'{path:60}' - [PEND] Needs download")
                download_tasks.append(entry)

    if not download_tasks:
        print("\nTodos os arquivos já estão baixados")
        return

    print(f"\nIniciando download de {len(download_tasks)} arquivos...")
    
    for idx, task in enumerate(download_tasks, 1):
        print(f"\n[{idx}/{len(download_tasks)}] Iniciando: {task['path_save_file']}")
        download_file_with_retry(
            file_name=os.path.basename(task['path_save_file']),
            url=task['link_to_download'],
            save_path=task['path_save_file'],
            file_size=task['file_size_bytes'],
            global_start=started_at
        )

def valid_file_exists(path):
    """Verifica se o arquivo existe e é um ZIP válido"""
    try:
        with zipfile.ZipFile(path, 'r') as z:
            return z.testzip() is None
    except (FileNotFoundError, zipfile.BadZipFile):
        return False

def download_file_with_retry(file_name, url, save_path, file_size, global_start):
    """Tenta baixar o arquivo com mecanismo de retentativa"""
    if file_size <= 0:
        print(f"⚠️ Tamanho do arquivo não disponível. Baixando sem informações de progresso: {file_name}")
    
    session = requests.Session()
    
    # Configurar política de retentativa
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods=frozenset(['GET'])
    )
    
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, headers=headers, timeout=TIMEOUT) as response:
                response.raise_for_status()
                download_stream(response, save_path, file_size, global_start, file_name)
                return  # Sucesso
                
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            print(f"\n⚠️ Erro na tentativa {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES:
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                print(f"🕒 Aguardando {wait_time:.1f}s antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                print(f"❌ Falha definitiva após {MAX_RETRIES} tentativas")
                raise

def download_stream(response, save_path, file_size, global_start, file_name):
    """Processa o fluxo de download com monitoramento"""
    downloaded = 0
    start_time = time.time()
    
    with open(save_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:  # Filtrar keep-alive chunks
                f.write(chunk)
                downloaded += len(chunk)
                
                # Atualizar progresso
                update_progress(
                    file_name=file_name,
                    downloaded=downloaded,
                    total=file_size,
                    start_time=start_time,
                    global_start=global_start
                )

def update_progress(file_name, downloaded, total, start_time, global_start):
    """Atualiza e exibe o progresso do download em megabytes."""
    elapsed = time.time() - start_time
    global_elapsed = time.time() - global_start
    
    # Converte bytes para megabytes
    downloaded_mb = downloaded / (1024 * 1024)
    total_mb = total / (1024 * 1024) if total > 0 else 0
    
    if total <= 0:
        # Se o tamanho do arquivo não estiver disponível, exibe apenas os MB baixados
        print(f"📥 Baixando {file_name}: {downloaded_mb:.2f} MB")
        return
    
    speed = downloaded / elapsed if elapsed > 0 else 0  # Velocidade em bytes por segundo
    speed_mb = speed / (1024 * 1024)  # Converte a velocidade para MB/s
    eta = (total - downloaded) / speed if speed > 0 else 0  # Tempo restante em segundos
    
    dict_status[file_name] = {
        'total_completed_bytes': downloaded,
        'file_size_bytes': total,
        'pct_downloaded': downloaded / total,
        'started_at': global_start,
        'running_time_seconds': global_elapsed,
        'speed': speed_mb,  # Velocidade em MB/s
        'eta': eta,
    }
    
    # Exibe o progresso em MB
    print(
        f"📥 Baixando {file_name}: {downloaded_mb:.2f} MB / {total_mb:.2f} MB "
        f"({downloaded / total * 100:.2f}%) - Velocidade: {speed_mb:.2f} MB/s"
    )

if __name__ == '__main__':
    main()
