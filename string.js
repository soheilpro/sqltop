function padLeft(nr, n, str) {
  return Array(n - String(nr).length + 1).join(str || ' ') + nr;
}

function truncate(text, maxLength) {
  if (text.length <= maxLength)
    return text;

  return text.substring(0, maxLength) + '...';
}

function makeProgressBar(value, length) {
  const fill = Math.floor((value || 1) * length);
  const empty = length - fill;

  return Array(empty + 1).join(' ') + Array(fill + 1).join('#');
}

module.exports = {
  padLeft: padLeft,
  truncate: truncate,
  makeProgressBar: makeProgressBar,
}
