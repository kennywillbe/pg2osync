// Force Rust theme — picker'da tek Rust bıraktık, ama localStorage'da eski navy kalmışsa onu da temizle
try {
  const cur = localStorage.getItem('mdbook-theme');
  if (cur && cur !== 'rust' && cur !== '"rust"') {
    localStorage.setItem('mdbook-theme', 'rust');
    // class'ı anında düzelt ki reload beklemeden renk değişsin
    document.documentElement.classList.remove('light','coal','navy','ayu');
    document.documentElement.classList.add('rust');
    // mdBook'un tema JS'i zaten çalıştı, bir kez reload et ki tam otursun
    if (!sessionStorage.getItem('rust-force-reloaded')) {
      sessionStorage.setItem('rust-force-reloaded', '1');
      location.reload();
    }
  } else if (!cur) {
    localStorage.setItem('mdbook-theme', 'rust');
  }
  sessionStorage.removeItem('rust-force-reloaded');
} catch(e) {}
