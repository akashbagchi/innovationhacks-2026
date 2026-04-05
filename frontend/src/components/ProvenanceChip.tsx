import { useState, useEffect } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from './ui/dialog'

interface ProvenanceChipProps {
  payer: string
  policyTitle: string
  policyId?: string | null
  effectiveDate?: string
  section?: string        // indication or section name
  excerpt?: string        // verbatim criterion text(s)
  pageNumber?: number | null
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }

export function ProvenanceChip({
  payer,
  policyTitle,
  policyId,
  effectiveDate,
  section,
  excerpt,
  pageNumber,
}: ProvenanceChipProps) {
  const [open, setOpen] = useState(false)
  const [pdfUrl, setPdfUrl] = useState<string | null>(null)
  const [pdfLoading, setPdfLoading] = useState(false)
  const [pdfError, setPdfError] = useState(false)

  // Lazy-fetch presigned PDF URL when dialog is opened
  useEffect(() => {
    if (!open || pdfUrl || pdfLoading || pdfError) return

    const params = new URLSearchParams()
    if (payer) params.set('payer', payer)
    if (policyId) params.set('policy_id', policyId)
    if (!payer && !policyId) return

    setPdfLoading(true)
    fetch(`/api/policies/document-url?${params.toString()}`)
      .then(r => {
        if (!r.ok) throw new Error('not found')
        return r.json()
      })
      .then(data => setPdfUrl(data.url))
      .catch(() => setPdfError(true))
      .finally(() => setPdfLoading(false))
  }, [open, payer, policyId, pdfUrl, pdfLoading, pdfError])

  const pageLabel = pageNumber != null ? `p. ${pageNumber}` : 'p. —'

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        style={{ ...mono, display: 'inline-flex', alignItems: 'center', gap: '4px', padding: '2px 7px', borderRadius: '1px', fontSize: '10px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845', cursor: 'pointer', transition: 'border-color 0.1s' }}
        onMouseEnter={e => (e.currentTarget.style.borderColor = '#91bfeb')}
        onMouseLeave={e => (e.currentTarget.style.borderColor = '#D8D4CC')}
      >
        <span>{payer}</span>
        <span style={{ color: '#D8D4CC' }}>|</span>
        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '140px' }}>{policyTitle}</span>
        <span style={{ color: '#D8D4CC' }}>|</span>
        <span>{pageLabel}</span>
      </button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent style={{ maxWidth: '480px', borderRadius: '2px', border: '1px solid #D8D4CC', background: '#FFFFFF', padding: 0, color: '#131210' }}>
          <div style={{ borderBottom: '1px solid #D8D4CC', padding: '16px 20px' }}>
            <DialogHeader>
              <DialogTitle style={{ ...mono, fontSize: '11px', letterSpacing: '0.15em', textTransform: 'uppercase', color: '#918D88', fontWeight: 400 }}>Source Provenance</DialogTitle>
            </DialogHeader>
          </div>
          <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
              {[
                { label: 'Payer',       value: payer },
                { label: 'Policy',      value: policyTitle },
                { label: 'Policy ID',   value: policyId ?? '—' },
                { label: 'Effective',   value: effectiveDate ?? '—' },
                { label: 'Section',     value: section ?? 'Coverage Criteria' },
                { label: 'Page',        value: pageNumber != null ? `p. ${pageNumber}` : '—' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.15em', textTransform: 'uppercase', color: '#918D88', marginBottom: '3px' }}>{label}</p>
                  <p style={{ fontSize: '12px', fontWeight: 600, color: '#131210', wordBreak: 'break-word' }}>{value}</p>
                </div>
              ))}
            </div>

            {excerpt && (
              <div style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '10px 12px' }}>
                <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.15em', textTransform: 'uppercase', color: '#918D88', marginBottom: '6px' }}>Source excerpt</p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {excerpt.split('\n\n').map((text, i) => (
                    <p key={i} style={{ fontSize: '11px', color: '#4A4845', lineHeight: 1.5 }}>{text}</p>
                  ))}
                </div>
              </div>
            )}

            {pdfLoading ? (
              <span style={{ ...mono, fontSize: '10px', color: '#918D88' }}>Loading document link…</span>
            ) : pdfUrl ? (
              <a
                href={pdfUrl}
                target="_blank"
                rel="noopener noreferrer"
                style={{ ...mono, fontSize: '10px', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#91bfeb', textDecoration: 'none' }}
              >
                View source PDF →
              </a>
            ) : (
              <span style={{ ...mono, fontSize: '10px', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#D8D4CC' }}>
                {pdfError ? 'Source PDF not available' : 'View PDF'}
              </span>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}
