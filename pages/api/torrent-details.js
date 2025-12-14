// This feature is decommissioned because the 'webtorrent' dependency has been removed.
export default function handler(req, res) {
  res.status(501).json({ 
    error: 'This feature is no longer supported.',
    detail: 'The functionality to fetch torrent details has been disabled.' 
  });
}
