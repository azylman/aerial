export function log(message: string, level: 'info' | 'error' = 'info') {
    process.stderr.write(`[${level}] ${message}\n`);
}

export function info(message: string) {
    log(message, 'info');
}

export function error(message: string) {
    log(message, 'error');
}

