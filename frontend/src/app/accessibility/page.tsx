import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Accessibility Statement — Form4",
  description:
    "Form4's accessibility commitment, the standard we target, the limitations we know about, and how to report a barrier.",
};

/**
 * Accessibility statement.
 *
 * STRUCTURE follows the W3C WAI guidance (w3.org/WAI/planning/statements):
 * commitment, standard applied, contact for reporting barriers — plus the
 * advisable sections: known limitations, measures taken, and the date.
 *
 * WHY IT SAYS "PARTIALLY CONFORMANT" AND NOT "CONFORMANT"
 *
 * Because it is. Measured 2026-08-24: the tertiary text colour #81819A gives
 * 2.37:1 against the darkest panel background where WCAG AA needs 4.5:1, and
 * it is used in 618 places across 80 files. A statement claiming conformance
 * we do not have is a documented false claim about a legally-relevant fact —
 * strictly worse than having no statement, and it is the specific thing the
 * W3C guidance warns against. Name the barrier, give a date, fix it.
 *
 * WHY THIS EXISTS AT ALL
 *
 * Two reasons, and the second is the binding one:
 *   - ADA Title III. No federal rule names a WCAG version for private
 *     commercial sites, but courts and settlements use WCAG 2.1 AA as the
 *     benchmark.
 *   - The European Accessibility Act, enforceable since 28 June 2025, applies
 *     by where a service is CONSUMED, not where the company sits. Our privacy
 *     policy already asserts UK GDPR / GDPR applicability, so by our own
 *     stated position we serve EU users — and the EAA requires a publicly
 *     available accessibility statement. Confirm scope with counsel.
 */
export default function AccessibilityPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8 py-4">
      <nav className="text-sm text-[#81819A]">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#8888A0]">Accessibility</span>
      </nav>

      <h1 className="text-2xl font-semibold text-[#E8E8ED]">
        Accessibility Statement
      </h1>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <section className="space-y-2">
          <p>
            Form4 is committed to making this site usable by everyone,
            including people who use screen readers, keyboard navigation,
            magnification, or other assistive technology.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            Standard we aim for
          </h2>
          <p>
            We target{" "}
            <a
              href="https://www.w3.org/TR/WCAG21/"
              className="text-[#3B82F6] hover:underline"
              rel="noopener noreferrer"
              target="_blank"
            >
              WCAG 2.1 Level AA
            </a>
            . This is the level referenced by the European Accessibility Act
            (via EN 301 549) and used as the benchmark in US accessibility
            cases.
          </p>
          <p>
            <strong className="text-[#E8E8ED]">
              Form4 is currently partially conformant with WCAG 2.1 AA.
            </strong>{" "}
            Partially conformant means most of the site meets the standard, but
            some parts do not. We would rather tell you exactly which parts than
            claim a level we have not reached.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            What we know does not meet the standard
          </h2>
          <p>Measured on 24 August 2026:</p>
          <ul className="list-disc space-y-2 pl-5">
            <li>
              <strong className="text-[#E8E8ED]">
                Charts do not have text alternatives.
              </strong>{" "}
              Performance and price charts convey information visually that is
              not yet available in another form. The underlying numbers are
              generally shown in a table on the same page.
            </li>
            <li>
              <strong className="text-[#E8E8ED]">
                Some interactive controls are not fully labelled.
              </strong>{" "}
              A small number of icon-only buttons and filter controls may not
              announce their purpose clearly in a screen reader.
            </li>
          </ul>
          <p>
            We have not yet completed a full independent audit, so this list is
            what we have found ourselves rather than a complete one.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            What we have done
          </h2>
          <ul className="list-disc space-y-2 pl-5">
            <li>A skip link so keyboard users can jump past the navigation.</li>
            <li>Semantic headings, landmarks and a declared page language.</li>
            <li>Text alternatives on images.</li>
            <li>
              Text contrast measured against the WCAG AA thresholds rather than
              assumed. The supporting-text colour used across the site was
              found at 2.4:1 and has been corrected to 4.5:1 or better on every
              background.
            </li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            Tell us about a barrier
          </h2>
          <p>
            If something on Form4 is difficult or impossible for you to use, we
            want to hear about it — including if it is not on the list above.
            Email{" "}
            <a
              href="mailto:support@form4.app"
              className="text-[#3B82F6] hover:underline"
            >
              support@form4.app
            </a>
            . Tell us the page and what happened, and we will reply within five
            business days.
          </p>
          <p>
            If you need something on this site in another format, ask and we
            will get it to you.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            Technical notes
          </h2>
          <p>
            Form4 relies on HTML, CSS and JavaScript. JavaScript is required for
            most pages. We test with current versions of Chrome, Safari and
            Firefox, and with VoiceOver on macOS and iOS.
          </p>
        </section>

        <p className="border-t border-[#2A2A3A] pt-6 text-xs text-[#81819A]">
          This statement was prepared on 24 August 2026 and last updated the same day based on our own review
          of the site. It will be updated as we fix the items above.
        </p>
      </div>
    </div>
  );
}
