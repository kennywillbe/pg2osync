// Anyone who chose another theme before the picker was reduced to Rust still has
// it in localStorage, and mdBook would keep honouring it. Migrate that once; new
// readers get `default-theme` from book.toml and never reach this branch.
try {
  const stored = localStorage.getItem('mdbook-theme');
  if (stored && stored !== 'rust' && stored !== '"rust"') {
    localStorage.setItem('mdbook-theme', 'rust');
    document.documentElement.classList.remove('light', 'coal', 'navy', 'ayu');
    document.documentElement.classList.add('rust');
  }
} catch (e) {
  // a browser with storage disabled renders the default theme, which is Rust
}
