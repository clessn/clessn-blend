var host = "kokkai.ndl.go.jp";
var referrer = "120815104X00520220407";
var url = "https://kokkai.ndl.go.jp/#/detail?minId=120815104X00520220407";
if (referrer.indexOf(host) === -1 && performance.navigation.type !== 1 && url.indexOf('print') === -1 && url.indexOf('detailRead') === -1) {
  localStorage.removeItem("vuex");
}
window.addEventListener('load', function() {
  var cookieEnabledFlg = false;
  if (navigator.cookieEnabled) {
    // クッキー 有効時
    // IE,Edge対応
    if (checkCookie()) {
      // クッキー有効
      cookieEnabledFlg = true;
    }
  }
  if (cookieEnabledFlg === false) {
    // クッキー 無効時
    // <div id="alert">このシステムでは、画面を移動しても入力した語や検索式などが残るよう、ブラウザにCookieを保存します。Cookieをオンにしてご利用ください。</div> を生成
    var elemAlert = document.createElement('div');
    elemAlert.setAttribute('id', 'cookieAlert');
    var message = 'このシステムでは、画面を移動しても入力した語や検索式などが残るよう、ブラウザにCookieを保存します。Cookieをオンにしてご利用ください。';
    var textAlert = document.createTextNode(message);
    elemAlert.appendChild(textAlert);
    document.body.insertBefore(elemAlert, document.body.firstElementChild);
    alert(message);
  } else {
    // 有効時
    var elem = document.getElementById('cookieAlert');
    if (elem != null) {
      elem.parentNode.removeChild(elem);
    }
  }
});

function checkCookie() {
  var writeCookie = new Date().getMilliseconds();
  $.cookie("milliseconds", writeCookie);
  var readCookie = $.cookie("milliseconds");
  return (!$.isEmptyObject(readCookie) && (writeCookie == readCookie))
}
