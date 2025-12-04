import { insertTorrentDetails } from '../../lib/db';

const TRACKERS = [
  'udp://tracker.opentrackr.org:1337/announce',
  'udp://tracker.torrent.eu.org:451/announce',
  'udp://tracker.qu.ax:6969/announce',
  'udp://tracker.fnix.net:6969/announce',
  'udp://evan.im:6969/announce',
  'udp://martin-gebhardt.eu:25/announce',
  'udp://extracker.dahrkael.net:6969/announce',
  'udp://tracker.skynetcloud.site:6969/announce',
  'udp://d40969.acod.regrucolo.ru:6969/announce',
  'udp://open.demonoid.ch:6969/announce',
  'wss://tracker.openwebtorrent.com/announce',
  'wss://tracker.btorrent.xyz/announce',
  'wss://tracker.fastcast.nz/announce',
  'wss://tracker.webtorrent.io/announce',
  'ws://tracker.btorrent.xyz:6969/announce',
  'ws://tracker.webtorrent.io:443/announce',
  'https://tracker.yemekyedim.com:443/announce',
  'https://tracker.alaskantf.com:443/announce',
  'https://tracker.qingwa.pro:443/announce',
  'https://tracker.moeblog.cn:443/announce',
  'https://tracker.ghostchu-services.top:443/announce',
  'https://tracker.uraniumhexafluori.de:443/announce',
  'https://tracker.pmman.tech:443/announce',
];

class NullStore {
  constructor (chunkLength, opts) { this.chunkLength = chunkLength }
  put (index, buf, cb) { if (cb) cb(null) }
  get (index, opts, cb) {
    if (typeof opts === 'function') cb = opts
    if (cb) cb(new Error('Storage disabled'))
  }
  close (cb) { if (cb) cb(null) }
  destroy (cb) { if (cb) cb(null) }
}

const parseTorrentInfo = (torrent) => {
  const name = torrent.name || '';
  const nameLower = name.toLowerCase();

  // 1. Resolution
  let resolution = 'N/A';
  if (nameLower.includes('2160p') || nameLower.includes('4k')) resolution = '4K';
  else if (nameLower.includes('1080p')) resolution = '1080p';
  else if (nameLower.includes('720p')) resolution = '720p';

  // 2. HDR/SDR
  let hdr_type = 'SDR';
  const isHdr = /\bhdr\b/.test(nameLower) ||
                /\bhdr10\b/.test(nameLower) ||
                /\bdolby\s?vision\b/.test(nameLower) ||
                /\bdv\b/.test(nameLower);

  if (isHdr) {
      hdr_type = 'HDR';
  }

  // 3. File Type (from largest file)
  let file_type = 'N/A';
  if (torrent.files && torrent.files.length > 0) {
      try {
          const largestFile = torrent.files.reduce((prev, curr) => 
              (prev.length > curr.length) ? prev : curr
          );
          const parts = largestFile.name.split('.');
          if (parts.length > 1) {
              file_type = parts.pop().toLowerCase();
          }
      } catch (e) {
          file_type = 'unknown';
      }
  }

  return {
    resolution,
    size: torrent.length,
    files: torrent.files ? torrent.files.map(f => f.name) : [],
    hdr_type,
    file_type,
  };
};

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { magnet } = req.query;
  if (!magnet || typeof magnet !== 'string') {
    return res.status(400).json({ error: 'Magnet link is required' });
  }

  const infoHashMatch = magnet.match(/btih:([a-fA-F0-9]{40})/);
  const info_hash = infoHashMatch ? infoHashMatch[1].toUpperCase() : null;

  if (!info_hash) {
    return res.status(400).json({ error: 'Invalid magnet link: info_hash not found' });
  }

  const { default: WebTorrent } = await import('webtorrent');
  const client = new WebTorrent();
  const fullMagnet = `${magnet}&tr=${TRACKERS.join('&tr=')}`;
  
  const options = {
    store: NullStore,
    skipVerify: true,
  };

  const timeout = setTimeout(() => {
    if (!res.headersSent) {
      res.status(504).json({ error: 'Timeout: Could not retrieve torrent metadata' });
      try {
        client.destroy();
      } catch(e) {}
    }
  }, 60000); // 60 seconds

  client.on('error', (err) => {
    // Suppress client-level errors, as we handle them per torrent.
  });

  client.add(fullMagnet, options, (torrent) => {
    torrent.on('metadata', () => {
      const info = parseTorrentInfo(torrent);
      
      try {
        insertTorrentDetails({ info_hash, ...info });
      } catch (dbError) {
        console.error("Failed to cache torrent details:", dbError);
      }

      if (!res.headersSent) {
        res.status(200).json(info);
      }
      
      clearTimeout(timeout);
      try {
        client.destroy();
      } catch(e) {}
    });

    torrent.on('error', (err) => {
      if (!res.headersSent) {
        res.status(500).json({ error: 'Failed to fetch torrent metadata.' });
      }
      clearTimeout(timeout);
      try {
        client.destroy();
      } catch(e) {}
    });
  });
}
