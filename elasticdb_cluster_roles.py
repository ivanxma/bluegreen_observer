"""Discover cluster roles using the endpoints configured by the observer."""
from concurrent.futures import ThreadPoolExecutor


def discover_roles(config, connector):
    def sample(server):
        result = dict(server, connected=False, read_only=None, hostname='', server_uuid='', error='')
        con = None
        try:
            con = connector(dict(config, host=server['host'], port=server['port']))
            with con.cursor() as cur:
                cur.execute('SELECT @@hostname, @@server_uuid, @@global.read_only, @@global.super_read_only')
                hostname, server_uuid, read_only, super_read_only = cur.fetchone()
            result.update(connected=True, hostname=str(hostname), server_uuid=str(server_uuid),
                          read_only=bool(int(read_only) or int(super_read_only)))
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            password = config.get('password', '')
            result['error'] = error.replace(password, '[redacted]') if password else error
        finally:
            if con:
                try:
                    con.close()
                except Exception:
                    pass
        return result

    with ThreadPoolExecutor(max_workers=min(16, len(config['servers']))) as pool:
        return list(pool.map(sample, config['servers']))


def role_targets(roles, readonly):
    targets, seen = [], set()
    for server in roles:
        if server['connected'] and server['read_only'] is readonly and server['server_uuid'] not in seen:
            targets.append(server)
            seen.add(server['server_uuid'])
    if not readonly and len(targets) > 1:
        raise RuntimeError('Multiple different writable servers detected. Check Observer endpoints before writing.')
    if not targets:
        raise RuntimeError('No reachable read-only server detected.' if readonly else 'No reachable writable primary detected.')
    return targets


def require_role(con, readonly):
    with con.cursor() as cur:
        cur.execute('SELECT @@global.read_only, @@global.super_read_only')
        ro, super_ro = cur.fetchone()
    if bool(int(ro) or int(super_ro)) != readonly:
        raise RuntimeError('Server role changed; reconnecting through Observer endpoints.')
