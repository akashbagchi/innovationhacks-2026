import { useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from './ui/dialog'

interface ProvenanceChipProps {
  payer: string
  policyTitle: string
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }

export function ProvenanceChip({ payer, policyTitle }: ProvenanceChipProps) {
  const [open, setOpen] = useState(false)

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        style={{ ...mono, display: 'inline-flex', alignItems: 'center', gap: '4px', padding: '2px 7px', borderRadius: '1px', fontSize: '10px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845', cursor: 'pointer', transition: 'border-color 0.1s' }}
        onMouseEnter={e => (e.currentTarget.style.borderColor = '#EF5050')}
        onMouseLeave={e => (e.currentTarget.style.borderColor = '#D8D4CC')}
      >
        <span>{payer}</span>
        <span style={{ color: '#D8D4CC' }}>|</span>
        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '140px' }}>{policyTitle}</span>
        <span style={{ color: '#D8D4CC' }}>|</span>
        <span>p. —</span>
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
                { label: 'Payer',   value: payer },
                { label: 'Policy',  value: policyTitle },
                { label: 'Page',    value: '— (pipeline pending)' },
                { label: 'Section', value: 'Coverage Criteria' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.15em', textTransform: 'uppercase', color: '#918D88', marginBottom: '3px' }}>{label}</p>
                  <p style={{ fontSize: '12px', fontWeight: 600, color: '#131210' }}>{value}</p>
                </div>
              ))}
            </div>
            <div style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '10px 12px' }}>
              <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.15em', textTransform: 'uppercase', color: '#918D88', marginBottom: '4px' }}>Source excerpt</p>
              <p style={{ fontSize: '11px', color: '#918D88', fontStyle: 'italic', lineHeight: 1.5 }}>Source text excerpt will appear here once the extraction pipeline is connected.</p>
            </div>
            <button style={{ ...mono, fontSize: '10px', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#EF5050', background: 'none', border: 'none', cursor: 'pointer', padding: 0, textAlign: 'left' }}>
              View PDF (placeholder)
            </button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}
