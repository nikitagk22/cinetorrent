// This feature is decommissioned because the 'webtorrent' and 'm2t' dependencies have been removed.
export default function handler(req, res) {
  res.status(501).json({ 
    error: 'This feature is no longer supported.',
    detail: 'The functionality to convert magnet links to torrent files has been disabled.' 
  });
}

