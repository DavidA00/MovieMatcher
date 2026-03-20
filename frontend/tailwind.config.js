/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  theme: {
    extend: {
      colors: {
        surface: { 0: '#0a0a0f', 1: '#12121a', 2: '#1a1a26', 3: '#242434' },
        accent: { DEFAULT: '#6d5aff', light: '#8b7aff', dim: '#4a3dbf' },
        ember: '#ff6b4a',
        gold: '#fbbf24',
        sage: '#34d399',
      },
      fontFamily: {
        display: ['DM Sans', 'system-ui', 'sans-serif'],
        body: ['DM Sans', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
};
