import axios from 'axios';
import fs from 'fs';
import path from 'path';

const ARIA2_URL = 'http://127.0.0.1:6800/jsonrpc';
const CACHE_DIR = path.join(process.cwd(), 'torrent_cache');

if (!fs.existsSync(CACHE_DIR)) {
  try {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  } catch (e) {
    console.error('Не удалось создать папку кэша:', e);
  }
}

async function aria2Request(method, params = []) {
  const id = Date.now().toString();
  try {
    const payload = {
      jsonrpc: '2.0',
      id,
      method: `aria2.${method}`,
      params: params, 
    };

    const response = await axios.post(ARIA2_URL, payload);
    
    if (response.data.error) {
      throw new Error(`Aria2 RPC Error: ${response.data.error.message}`);
    }

    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data?.error?.message || error.message;
    // Скрываем лог, если ошибка "Active Download not found" при очистке (это не критично)
    if (!errorMsg.includes('Active Download not found')) {
        console.error(`[Aria2] Error in ${method}:`, errorMsg);
    }
    throw new Error(`Aria2 request failed: ${errorMsg}`);
  }
}

export async function getTorrentFile(magnetLink) {
  const match = magnetLink.match(/btih:([a-fA-F0-9]{40})/);
  if (!match) throw new Error('Invalid magnet link format');
  
  const infoHash = match[1].toLowerCase();
  const filePath = path.join(CACHE_DIR, `${infoHash}.torrent`);

  if (fs.existsSync(filePath)) {
    return filePath;
  }

  let gid = null;
  try {
    const addRes = await aria2Request('addUri', [
      [magnetLink], 
      { 
        'bt-metadata-only': 'true',
        'bt-save-metadata': 'true',
      }
    ]);
    
    gid = addRes.result;

    const timeout = 60000;
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      const statusRes = await aria2Request('tellStatus', [gid]);
      const status = statusRes.result.status;

      if (status === 'complete') {
        await new Promise(r => setTimeout(r, 100)); 
        
        if (fs.existsSync(filePath)) {
            // !!! ВОТ ЗДЕСЬ БЫЛА ОШИБКА, ИСПРАВЛЕНО НА removeDownloadResult !!!
            await aria2Request('removeDownloadResult', [gid]); 
            return filePath;
        }
      } 
      
      if (status === 'error' || status === 'removed') {
        throw new Error(`Download failed with status: ${status}`);
      }

      await new Promise(r => setTimeout(r, 1000));
    }

    throw new Error('Timeout: Metadata took too long to download');

  } catch (error) {
    if (gid) {
        // forceRemove удаляет активные, removeDownloadResult удаляет завершенные/ошибочные
        try { await aria2Request('forceRemove', [gid]); } catch(e) {}
        try { await aria2Request('removeDownloadResult', [gid]); } catch(e) {}
    }
    throw error;
  }
}