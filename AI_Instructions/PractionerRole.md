## PractitionerRole

In the three-tier architecture, **PractitionerRole** is the bridge object between a **Practitioner** and an **Organization**. In NDH, it is the resource that captures the provider’s role in relation to an organization, along with role-specific attributes such as specialty, contact information, service context, and validation signals. The NDH profile requires `active` and allows references to `practitioner`, `organization`, `location`, `healthcareService`, `telecom`, and `endpoint`. It also includes the NDH extensions for `newpatients`, `rating`, and `verification-status`. ([FHIR Build][1])

For CMS implementation, **PractitionerRole** should contain a standard integer identifier in the database plus a UUID-backed FHIR resource `id`. The record should include a link to exactly one practitioner and a link to exactly one organization. It should carry the **accepting-new-patients** extension, and that should be treated as a single business value tied to the role record. It should also include a single integer rating, a single active flag, and the three CMS-overloaded verification statuses using the same verification-status pattern you defined earlier. NDH permits a `verification-status` extension on PractitionerRole and defines `active` as mandatory. ([FHIR Build][1])

For terminology, **PractitionerRole.code** should contain **one and only one NDH practitioner role code** as a `CodeableConcept`. In the NDH profile, the role code slice binds to the **PractitionerRole Code Value Set** with required strength, while `specialty` is a separate repeatable field used for specialty concepts. `specialty` should therefore hold an array of **NUCC specialty taxonomy codes**, and credentials should **not** be carried here. NDH explicitly separates `code` from `specialty`, and `specialty` is repeatable. ([FHIR Build][1])

For service context, CMS should constrain the FHIR model down to **one location** and **one healthcare service**, even though the NDH profile allows both to repeat. Telecom should be restricted to **phone and fax only**, excluding email, even though the base contact point system permits email. This is a CMS implementation rule, not an NDH limitation. ([FHIR Build][1])

So the CMS PractitionerRole profile-in-practice is: one practitioner, one organization, one NDH practitioner role code, many specialties, one new-patients value, one rating, one active flag, the three CMS verification statuses, one location, one healthcare service, and telecom limited to phone and fax. That keeps the bridge table narrow while remaining aligned to the NDH structure. ([FHIR Build][1])



