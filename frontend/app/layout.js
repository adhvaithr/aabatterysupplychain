import './globals.css'
import { DM_Mono } from 'next/font/google'
import { GeistSans } from 'geist/font/sans'
import Providers from './providers'

const dmMono = DM_Mono({
  subsets: ['latin'],
  weight: ['400', '500'],
  variable: '--font-dm-mono',
})

export const metadata = {
  title: 'PoP Sentinel — Inventory Imbalance Detection',
  description: 'AI-powered inventory imbalance detection for CPG distributors',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en" className={`${GeistSans.variable} ${dmMono.variable}`}>
      <head>
        <script dangerouslySetInnerHTML={{__html:'window.addEventListener("error",function(e){if(e.error instanceof DOMException&&e.error.name==="DataCloneError"&&e.message&&e.message.includes("PerformanceServerTiming")){e.stopImmediatePropagation();e.preventDefault()}},true);'}} />
      </head>
      <body className="min-h-screen bg-[#0A0A0B] text-neutral-100 antialiased font-sans">
        <Providers>{children}</Providers>
      </body>
    </html>
  )
}
