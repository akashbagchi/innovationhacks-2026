import { useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from './ui/dialog'

interface ProvenanceChipProps {
  payer: string
  policyTitle: string
}

export function ProvenanceChip({ payer, policyTitle }: ProvenanceChipProps) {
  const [open, setOpen] = useState(false)

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-slate-100 text-slate-600 hover:bg-slate-200 transition-colors border border-slate-200 font-mono"
      >
        <span>{payer}</span>
        <span className="text-slate-400">|</span>
        <span className="truncate max-w-[140px]">{policyTitle}</span>
        <span className="text-slate-400">|</span>
        <span>Page —</span>
      </button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-sm font-mono">Source Provenance</DialogTitle>
          </DialogHeader>
          <div className="space-y-3 text-sm">
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div><span className="text-slate-500">Payer</span><p className="font-medium">{payer}</p></div>
              <div><span className="text-slate-500">Policy</span><p className="font-medium">{policyTitle}</p></div>
              <div><span className="text-slate-500">Page</span><p className="font-medium">— (pipeline pending)</p></div>
              <div><span className="text-slate-500">Section</span><p className="font-medium">Coverage Criteria</p></div>
            </div>
            <div className="bg-slate-50 rounded p-3 border border-slate-200">
              <p className="text-xs text-slate-500 mb-1">Source excerpt</p>
              <p className="text-slate-400 italic text-xs">Source text excerpt will appear here once the extraction pipeline is connected.</p>
            </div>
            <button className="text-xs text-blue-500 hover:underline">View PDF (placeholder)</button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}
