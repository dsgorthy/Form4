/**
 * The section heading used across every content surface.
 *
 * ONE DEFINITION, because there were EIGHT — insider, filing, company,
 * private company and explore pages plus three components each carried their
 * own copy, already drifting on margin (`mb-2` here, `mb-3` there). Restyling
 * the insider page created a ninth variant and made the drift visible.
 *
 * An <h2>, not a styled <div>. These label the real content sections of an
 * indexed page, and as divs they carried no document structure at all: every
 * SEO surface rendered exactly one heading, the H1, with nothing beneath it.
 *
 * IT CARRIES A RULE, and that is load-bearing rather than decorative. The
 * insider page had thirteen sections in one identical bordered container and
 * read, accurately, as dull and boxy. Removing the boxes only works if
 * something else separates the sections — this is that something. Add a
 * section, get a separator; no new border required.
 */
export function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="mb-3 border-b border-[#24242F] pb-2 font-mono text-[10.5px] font-medium uppercase tracking-[0.15em] text-[#63636F]">
      {children}
    </h2>
  );
}
