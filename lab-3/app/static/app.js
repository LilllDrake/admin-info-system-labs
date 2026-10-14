const byId = (id) => document.getElementById(id);
let refreshing = false;

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const data = await response.json();
  if (!response.ok) {
    const partial = Number.isInteger(data.created) ? ` Подтверждено записей: ${data.created}.` : '';
    throw new Error((data.error || `Ошибка HTTP ${response.status}`) + partial);
  }
  return data;
}

async function refreshOrders() {
  if (refreshing) return;
  refreshing = true;
  try {
    const data = await api('/api/orders');
    byId('source').textContent = `Источник: ${data.source}. Всего записей на нём: ${data.total}.`;
    byId('read-error').textContent = '';
    const body = byId('orders-body');
    body.replaceChildren();
    if (data.orders.length === 0) {
      const cell = body.insertRow().insertCell();
      cell.colSpan = 3;
      cell.textContent = 'На реплике пока нет записей.';
    }
    for (const order of data.orders) {
      const row = body.insertRow();
      for (const value of [order.id, order.description, new Date(order.created_at).toLocaleString('ru-RU')]) {
        row.insertCell().textContent = String(value);
      }
    }
  } catch (error) {
    byId('read-error').textContent = `${error.message} Показанный список может быть устаревшим.`;
  } finally {
    refreshing = false;
  }
}

byId('order-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const button = byId('create-button');
  button.disabled = true;
  try {
    const data = await api('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: byId('description').value }),
    });
    byId('message').textContent = `Primary подтвердил создание записи №${data.order.id}. Ожидаем её в списке с реплики.`;
  } catch (error) {
    byId('message').textContent = error.message;
  } finally {
    button.disabled = false;
    refreshOrders();
  }
});

byId('load-button').addEventListener('click', async () => {
  const count = Number(byId('load-count').value);
  if (!Number.isInteger(count) || count < 1 || count > 100000) {
    byId('message').textContent = 'Количество записей: целое число от 1 до 100000.';
    return;
  }
  const button = byId('load-button');
  button.disabled = true;
  byId('message').textContent = 'Создаём пачку записей на primary…';
  try {
    const data = await api('/api/load', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    });
    byId('message').textContent = `Создано ${data.created} записей за ${data.seconds} с. Последний ID: ${data.last_id}.`;
  } catch (error) {
    byId('message').textContent = error.message;
  } finally {
    button.disabled = false;
    refreshOrders();
  }
});

byId('refresh-button').addEventListener('click', refreshOrders);
refreshOrders();
setInterval(refreshOrders, 2000);
