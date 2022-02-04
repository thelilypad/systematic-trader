EPS = 10 ** -8
INDICES = [
    'DEFI',
    'MVDA10',
    'MVDA25',
    'WSB',
    'PRIV',
    'SHIT',
]

RESOLUTIONS_MAP = {
    '1h': 60 * 60,
    '1m': 60,
    '1d': 60 * 60 * 24,
}

LEVERAGED = [
    'BULL',
    'BEAR',
    'HALF',
    'HEDGE'
]

ELECTION = [
    'BOLSONARO',
    'TRUMP'
]

VOL_SPECIAL = [
    '-MOVE-',
    'IBVOL',
    'BVOL',
]

FIAT_CURRENCIES = [
    'CAD',
    'BRZ',
    'EUR',
    'GBP',
]

STABLECOINS = [
    'CUSDT',
    'DAI',
    'USDT',
]

BLACKLISTED = STABLECOINS + FIAT_CURRENCIES + VOL_SPECIAL + ELECTION + LEVERAGED + INDICES
