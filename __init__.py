import asyncio
import json
from homeassistant.core import HomeAssistant, State
from homeassistant.helpers.event import async_track_state_change
from homeassistant.const import EVENT_HOMEASSISTANT_START, EVENT_HOMEASSISTANT_STOP
import duckdb

DOMAIN = 'duckdb_history'
PLATFORM_SCHEMA = {}

async def async_setup(hass: HomeAssistant, config):
    """Set up the DuckDB history component."""
    hass.data[DOMAIN] = {}
    global conn
    conn = duckdb.connect('duckdb_history.db')
    conn.execute('CREATE TABLE IF NOT EXISTS history (entity_id TEXT, state TEXT, attributes TEXT, timestamps TIMESTAMP)')

    exclude_config = config.get(DOMAIN, {}).get('exclude', {})
    include_config = config.get(DOMAIN, {}).get('include', {})
    hass.data[DOMAIN]['exclude'] = exclude_config
    hass.data[DOMAIN]['include'] = include_config

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_close_db)

    async def async_entity_changed(event, old_state: State, new_state: State):
        if _should_record(hass, new_state.entity_id):
            entity = new_state.entity_id
            state = new_state.state
            attributes = json.dumps(new_state.attributes)
            timestamps = new_state.last_updated.timestamp()
            conn.execute('INSERT INTO history (entity_id, state, attributes, timestamps) VALUES (?, ?, ?, ?)', (entity, state, attributes, timestamps))
            conn.commit()

    async_track_state_change(hass, '*', async_entity_changed)
    return True

def _async_close_db(event=None):
    global conn
    conn.close()

def _should_record(hass, entity_id):
    exclude_config = hass.data[DOMAIN]['exclude']
    include_config = hass.data[DOMAIN]['include']

    if include_config:
        if entity_id in include_config.get('entities', []):
            return True
        if any(fnmatch.fnmatch(entity_id, pattern) for pattern in include_config.get('entity_globs', [])):
            return True
        domain = entity_id.split('.')[0]
        if domain in include_config.get('domains', []):
            return True
        return False

    if entity_id in exclude_config.get('entities', []):
        return False
    if any(fnmatch.fnmatch(entity_id, pattern) for pattern in exclude_config.get('entity_globs', [])):
        return False
    domain = entity_id.split('.')[0]
    if domain in exclude_config.get('domains', []):
        return False

    return True
