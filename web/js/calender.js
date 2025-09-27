// calendar.js
let currentDate = new Date();

function renderCalendar(year, month) {
  const calendar = document.getElementById("calendar");
  const monthYear = document.getElementById("monthYear");

  // 月の最初の日と最後の日
  const firstDay = new Date(year, month, 1);
  const lastDay = new Date(year, month + 1, 0);

  // 表のヘッダー
const weekDays = ["月", "火", "水", "木", "金", "土", "日"];
html = "<tr>";
weekDays.forEach((d, i) => {
  let className = "";
  if (i === 5) className = "sat"; // 土曜
  if (i === 6) className = "sun"; // 日曜
  html += `<th class="${className}">${d}</th>`;
});
html += "</tr><tr>";

// 最初の空白
let startDay = firstDay.getDay();
startDay = (startDay === 0) ? 6 : startDay - 1;
for (let i = 0; i < startDay; i++) {
  html += "<td></td>";
}

// 日付埋め込み
for (let d = 1; d <= lastDay.getDate(); d++) {
  const dateStr = `${year}${String(month+1).padStart(2,"0")}${String(d).padStart(2,"0")}`;
  let link = "";
  if (window.racedays && window.racedays.includes(dateStr)) {
    link = `<a href="../races/${dateStr}/index.html">${d}</a>`;
  } else {
    link = d;
  }

  html += `<td>${link}</td>`;

  if ((startDay + d) % 7 === 0) {
    html += "</tr><tr>";
  }
}

  html += "</tr>";
  calendar.innerHTML = html;

  // 見出し
  monthYear.textContent = `${year}年 ${month + 1}月`;
}

// ボタン制御
document.getElementById("prevMonth").onclick = () => {
  currentDate.setMonth(currentDate.getMonth() - 1);
  renderCalendar(currentDate.getFullYear(), currentDate.getMonth());
};
document.getElementById("nextMonth").onclick = () => {
  currentDate.setMonth(currentDate.getMonth() + 1);
  renderCalendar(currentDate.getFullYear(), currentDate.getMonth());
};

// 初期描画
renderCalendar(currentDate.getFullYear(), currentDate.getMonth());
