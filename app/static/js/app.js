// ============================================
// GLOBAL VARIABLES
// ============================================
let ws = null;
let reconnectAttempts = 0;
const maxReconnectAttempts = 5;
let priceChart = null;
const priceData = { labels: [], values: [] };

// ============================================
// DOM READY EVENT
// ============================================
document.addEventListener('DOMContentLoaded', function() {
    initPriceChart();
    document.getElementById('loginForm').addEventListener('submit', login);
    document.getElementById('logoutButton').addEventListener('click', logout);
    document.getElementById('orderType').addEventListener('change', togglePriceFields);
    document.getElementById('orderForm').addEventListener('submit', function(event) {
        event.preventDefault();
        placeOrder();
    });
    togglePriceFields();

    if (getToken()) {
        showApp();
    } else {
        showLogin();
    }
});

// ============================================
// AUTHENTICATION FUNCTIONS
// ============================================
function getToken() {
    return localStorage.getItem('token');
}

function authHeaders(extra) {
    extra = extra || {};
    return Object.assign({}, extra, { Authorization: 'Bearer ' + getToken() });
}

async function apiFetch(url, options) {
    options = options || {};
    const headers = authHeaders(options.headers || {});
    const response = await fetch(url, Object.assign({}, options, { headers: headers }));
    if (response.status === 401 || response.status === 403) {
        logout();
        throw new Error('Session expired');
    }
    return response;
}

async function login(event) {
    event.preventDefault();
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;
    const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, password: password })
    });
    const data = await response.json();
    if (!response.ok) {
        showAlert(data.detail || 'Login failed');
        return;
    }
    localStorage.setItem('token', data.access_token);
    showApp();
}

function logout() {
    localStorage.removeItem('token');
    if (ws) {
        ws.close();
        ws = null;
    }
    showLogin();
}

function showLogin() {
    document.getElementById('authPanel').classList.remove('hidden');
    document.getElementById('appShell').classList.add('hidden');
    document.getElementById('logoutButton').classList.add('hidden');
}

function showApp() {
    document.getElementById('authPanel').classList.add('hidden');
    document.getElementById('appShell').classList.remove('hidden');
    document.getElementById('logoutButton').classList.remove('hidden');
    loadAccountInfo();
    loadPositions();
    loadOpenOrders();
    connectWebSocket();
}

// ============================================
// CHART FUNCTIONS
// ============================================
function initPriceChart() {
    const ctx = document.getElementById('priceChart').getContext('2d');
    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: priceData.labels,
            datasets: [{
                label: 'Price (USDT)',
                data: priceData.values,
                borderColor: '#4F46E5',
                backgroundColor: 'rgba(79, 70, 229, 0.1)',
                fill: true,
                tension: 0.35,
                pointRadius: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { maxTicksLimit: 8 } },
                y: { position: 'right' }
            }
        }
    });
}

function updatePriceChart(price, timestamp) {
    if (!priceChart || !price) return;
    if (priceData.labels.length > 50) {
        priceData.labels.shift();
        priceData.values.shift();
    }
    priceData.labels.push(new Date(timestamp || Date.now()).toLocaleTimeString());
    priceData.values.push(price);
    priceChart.update();
}

// ============================================
// WEBSOCKET FUNCTIONS
// ============================================
function connectWebSocket() {
    if (!getToken()) return;
    if (ws) ws.close();
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + window.location.host + '/ws/trade?token=' + encodeURIComponent(getToken());
    ws = new WebSocket(wsUrl);

    ws.onopen = function() {
        reconnectAttempts = 0;
        subscribeToStreams();
    };
    ws.onmessage = function(event) {
        handleWebSocketMessage(JSON.parse(event.data));
    };
    ws.onclose = function() {
        attemptReconnect();
    };
    ws.onerror = function() {
        showAlert('WebSocket connection error');
    };
}

function subscribeToStreams() {
    sendWs({ action: 'subscribe', type: 'ticker', symbol: document.getElementById('chartSymbol').value });
    sendWs({
        action: 'subscribe',
        type: 'kline',
        symbol: document.getElementById('chartSymbol').value,
        interval: document.getElementById('chartInterval').value
    });
}

function sendWs(message) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
    }
}

function handleWebSocketMessage(data) {
    if (data.type === 'ticker') {
        updatePriceChart(data.price, data.timestamp);
    } else if (data.type === 'kline') {
        updatePriceChart(data.close, data.end_time);
    } else if (data.type === 'user_data' && data.event_type === 'ORDER_TRADE_UPDATE') {
        loadOpenOrders();
        loadPositions();
    } else if (data.type === 'error') {
        showAlert(data.message);
    }
}

function attemptReconnect() {
    if (!getToken() || reconnectAttempts >= maxReconnectAttempts) return;
    reconnectAttempts++;
    setTimeout(connectWebSocket, Math.pow(2, reconnectAttempts) * 1000);
}

setInterval(function() {
    sendWs({ action: 'ping', timestamp: Date.now() });
}, 30000);

// ============================================
// ORDER FORM FUNCTIONS
// ============================================
function togglePriceFields() {
    const orderType = document.getElementById('orderType').value;
    const fields = ['priceField', 'stopPriceField', 'stopLimitPriceField', 'callbackRateField', 'activationPriceField'];
    
    for (var i = 0; i < fields.length; i++) {
        document.getElementById(fields[i]).classList.add('hidden');
    }

    if (orderType === 'LIMIT' || orderType === 'STOP_LIMIT' || orderType === 'OCO') {
        document.getElementById('priceField').classList.remove('hidden');
    }
    if (orderType === 'STOP_LIMIT' || orderType === 'OCO') {
        document.getElementById('stopPriceField').classList.remove('hidden');
    }
    if (orderType === 'OCO') {
        document.getElementById('stopLimitPriceField').classList.remove('hidden');
    }
    if (orderType === 'TRAILING_STOP_MARKET') {
        document.getElementById('callbackRateField').classList.remove('hidden');
        document.getElementById('activationPriceField').classList.remove('hidden');
    }
}

function optionalFloat(id) {
    const value = document.getElementById(id).value;
    return value ? parseFloat(value) : null;
}

async function placeOrder() {
    const orderData = {
        symbol: document.getElementById('symbol').value,
        side: document.getElementById('side').value,
        order_type: document.getElementById('orderType').value,
        quantity: parseFloat(document.getElementById('quantity').value),
        price: optionalFloat('price'),
        stop_price: optionalFloat('stopPrice'),
        stop_limit_price: optionalFloat('stopLimitPrice'),
        callback_rate: optionalFloat('callbackRate'),
        activation_price: optionalFloat('activationPrice')
    };

    try {
        const response = await apiFetch('/api/orders/place', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(orderData)
        });
        const result = await response.json();
        if (!response.ok) {
            showAlert(result.detail || 'Order failed');
            return;
        }
        showAlert('Order placed successfully', 'success');
        loadOpenOrders();
    } catch (error) {
        showAlert(error.message);
    }
}

async function cancelOrder(orderId, symbol) {
    const response = await apiFetch('/api/orders/cancel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ order_id: orderId, symbol: symbol })
    });
    if (response.ok) {
        showAlert('Order cancelled successfully', 'success');
        loadOpenOrders();
    }
}

// ============================================
// ACCOUNT INFO FUNCTION
// ============================================
async function loadAccountInfo() {
    const accountInfoDiv = document.getElementById('accountInfo');
    if (!accountInfoDiv) return;
    
    accountInfoDiv.innerHTML = `
        <div class="flex flex-col"><span class="text-gray-400 text-xs">Balance</span><span class="font-semibold text-gray-800 animate-pulse">Loading...</span></div>
        <div class="flex flex-col"><span class="text-gray-400 text-xs">Equity</span><span class="font-semibold text-gray-800 animate-pulse">Loading...</span></div>
        <div class="flex flex-col"><span class="text-gray-400 text-xs">Unrealized P&L</span><span class="font-semibold animate-pulse">Loading...</span></div>
        <div class="flex flex-col"><span class="text-gray-400 text-xs">Available</span><span class="font-semibold animate-pulse">Loading...</span></div>
    `;
    
    try {
        const response = await apiFetch('/api/account/balance');
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Error loading account info');
        
        const balance = data.balance;
        const walletBalance = parseFloat(balance.totalWalletBalance || 0);
        const equity = parseFloat(balance.totalMarginBalance || 0);
        const unrealizedPnl = parseFloat(balance.totalUnrealizedProfit || 0);
        const available = parseFloat(balance.availableBalance || 0);
        
        const pnlClass = unrealizedPnl >= 0 ? 'text-green-600' : 'text-red-600';
        const pnlSymbol = unrealizedPnl >= 0 ? '+' : '';
        
        accountInfoDiv.innerHTML = `
            <div class="flex flex-col p-3 bg-gray-50 rounded-lg">
                <span class="text-gray-500 text-xs uppercase tracking-wide">Balance</span>
                <span class="font-bold text-gray-900 text-sm">$${walletBalance.toFixed(2)}</span>
            </div>
            <div class="flex flex-col p-3 bg-gray-50 rounded-lg">
                <span class="text-gray-500 text-xs uppercase tracking-wide">Equity</span>
                <span class="font-bold text-gray-900 text-sm">$${equity.toFixed(2)}</span>
            </div>
            <div class="flex flex-col p-3 bg-gray-50 rounded-lg">
                <span class="text-gray-500 text-xs uppercase tracking-wide">Unrealized P&L</span>
                <span class="font-bold text-sm ${pnlClass}">${pnlSymbol}$${unrealizedPnl.toFixed(2)}</span>
            </div>
            <div class="flex flex-col p-3 bg-gray-50 rounded-lg">
                <span class="text-gray-500 text-xs uppercase tracking-wide">Available</span>
                <span class="font-bold text-gray-900 text-sm">$${available.toFixed(2)}</span>
            </div>
        `;
    } catch (error) {
        accountInfoDiv.innerHTML = `
            <div class="col-span-4 text-center text-red-500 text-sm p-4">Error loading account info</div>
        `;
    }
}

// ============================================
// POSITIONS TABLE FUNCTIONS (SEPARATE)
// ============================================
async function loadPositions() {
    const tableBody = document.getElementById('positionsTableBody');
    if (!tableBody) return;
    
    tableBody.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-gray-400 text-sm"><div class="flex justify-center"><div class="animate-pulse">Loading positions...</div></div></td></tr>';
    
    try {
        const response = await apiFetch('/api/account/positions');
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Error loading positions');
        
        const positions = data.positions.filter(function(position) {
            return parseFloat(position.positionAmt) !== 0;
        });
        
        if (positions.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-gray-400 text-sm">No open positions</td></tr>';
            return;
        }
        
        renderPositionsTable(positions, tableBody);
    } catch (error) {
        tableBody.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-red-500 text-sm">Error loading positions</td></tr>';
    }
}

function renderPositionsTable(positions, tableBody) {
    var html = '';
    
    for (var i = 0; i < positions.length; i++) {
        var position = positions[i];
        var pnl = parseFloat(position.unRealizedProfit);
        var pnlClass = pnl >= 0 ? 'text-green-600' : 'text-red-600';
        var pnlSymbol = pnl >= 0 ? '▲' : '▼';
        
        html += `
            <tr class="hover:bg-gray-50 transition-colors">
                <td class="pl-10 pr-3 px-3 py-2 whitespace-nowrap text-sm font-medium text-gray-900">${position.symbol}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm text-gray-700">${parseFloat(position.positionAmt).toFixed(4)}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm text-gray-700">$${parseFloat(position.entryPrice).toFixed(2)}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm font-semibold ${pnlClass}">${pnlSymbol} $${Math.abs(pnl).toFixed(2)}</td>
            </tr>
        `;
    }
    
    tableBody.innerHTML = html;
}

// ============================================
// ORDERS TABLE FUNCTIONS (SEPARATE)
// ============================================
async function loadOpenOrders() {
    const tableBody = document.getElementById('ordersTableBody');
    if (!tableBody) return;
    
    tableBody.innerHTML = '<tr><td colspan="7" class="px-4 py-8 text-center text-gray-400 text-sm"><div class="flex justify-center"><div class="animate-pulse">Loading orders...</div></div></td></tr>';
    
    try {
        const response = await apiFetch('/api/orders/open');
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Error loading orders');
        
        if (data.orders.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="7" class="px-4 py-8 text-center text-gray-400 text-sm">No open orders</td></tr>';
            return;
        }
        
        renderOrdersTable(data.orders, tableBody);
    } catch (error) {
        tableBody.innerHTML = '<tr><td colspan="7" class="px-4 py-8 text-center text-red-500 text-sm">Error loading orders</td></tr>';
    }
}

function renderOrdersTable(orders, tableBody) {
    var html = '';
    
    for (var i = 0; i < orders.length; i++) {
        var order = orders[i];
        var sideClass = order.side === 'BUY' ? 'text-green-600' : 'text-red-600';
        var sideBg = order.side === 'BUY' ? 'bg-green-50' : 'bg-red-50';
        
        html += `
            <tr class="hover:bg-gray-50 transition-colors">
                <td class="pl-10 pr-3 px-3 py-2 whitespace-nowrap text-sm font-medium text-gray-900">${order.symbol}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm">
                    <span class="px-2 py-1 rounded-full text-xs font-semibold ${sideClass} ${sideBg}">${order.side}</span>
                </td>
                <td class="px-3 py-2 whitespace-nowrap text-sm text-gray-700">${order.type}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm text-gray-700">${parseFloat(order.origQty).toFixed(4)}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm text-gray-700">${order.price ? '$' + parseFloat(order.price).toFixed(2) : '—'}</td>
                <td class="px-3 py-2 whitespace-nowrap text-sm">
                    <span class="px-2 py-1 rounded-full text-xs font-semibold bg-yellow-50 text-yellow-700">${order.status}</span>
                </td>
                <td class="px-3 py-2 whitespace-nowrap text-sm">
                    <button onclick="cancelOrder('${order.orderId}', '${order.symbol}')" class="text-red-600 hover:text-red-800 transition-colors text-xs font-medium px-2 py-1 rounded hover:bg-red-50">
                        Cancel
                    </button>
                </td>
            </tr>
        `;
    }
    
    tableBody.innerHTML = html;
}

// ============================================
// ALERT FUNCTION
// ============================================
function showAlert(message, type) {
    type = type || 'error';
    const alertArea = document.getElementById('alertArea');
    alertArea.className = 'mb-6 p-4 rounded-lg ' + (type === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800');
    alertArea.textContent = message;
    alertArea.classList.remove('hidden');
    setTimeout(function() {
        alertArea.classList.add('hidden');
    }, 5000);
}