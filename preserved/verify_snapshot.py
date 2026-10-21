from pathlib import Path
import hashlib,json,zipfile

root=Path(__file__).resolve().parent
manifest=json.loads((root/'snapshot-manifest.json').read_text(encoding='utf-8'))
archive=root/manifest['archive']
sha=lambda b:hashlib.sha256(b).hexdigest()
assert sha(archive.read_bytes())==manifest['archive_sha256'],'Archive hash mismatch'
with zipfile.ZipFile(archive) as z:
    expected={r['path'] for r in manifest['files']}
    assert set(z.namelist())==expected,'Archive member list changed'
    for r in manifest['files']:
        b=z.read(r['path'])
        assert len(b)==r['bytes'] and sha(b)==r['sha256'],'File mismatch: '+r['path']
print('Verified archive and all',len(manifest['files']),'package files.')
