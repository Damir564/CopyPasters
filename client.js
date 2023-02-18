var source = new EventSource('http://localhost/client.py');
source.addEventListener('message', function(e) {
  // Пришли какие-то данные
  console.log(e.data);
}, false);

source.addEventListener('open', function(e) {
  // Соединение было открыто
}, false);

source.addEventListener('error', function(e) {
  if (e.eventPhase == EventSource.CLOSED) {
    // Соединение закрыто
  }
}, false);
