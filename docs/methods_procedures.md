# Processing Pipeline

This section describes the automated MRI processing pipeline implemented at the Strauss Neuroplasticity Brain Bank (SNBB). All processing stages were orchestrated by a custom rule-based scheduler that submitted jobs to a Slurm high-performance computing cluster, ensuring reproducibility and minimal manual intervention. The pipeline comprised six sequential stages executed in dependency order: (1) DICOM to BIDS conversion, (2) post-conversion fieldmap processing, (3) anatomical image defacing, (4) diffusion MRI preprocessing, (5) cortical surface reconstruction, and (6) diffusion MRI reconstruction and tractography. All neuroimaging tools were executed within Apptainer containers to guarantee version-locked, reproducible environments across compute nodes. Data were organized according to the Brain Imaging Data Structure (BIDS) specification throughout all stages (Gorgolewski et al., 2016).

## DICOM to BIDS Conversion

Raw DICOM files were converted to NIfTI format and organized into a BIDS-compliant directory structure using HeuDiConv 1.3.4 (Halchenko et al., 2024) with dcm2niix as the conversion backend (Li et al., 2016). A custom Python heuristic file mapped Siemens scanner protocol names to BIDS-compliant filenames and directory structures. The heuristic supported both current (2024+) and legacy protocol naming conventions used at the scanning site. Converted modalities included T1-weighted MPRAGE, T2-weighted SPC, FLAIR, multi-shell diffusion MRI (anterior–posterior and posterior–anterior phase-encoding directions), spin-echo field maps for functional distortion correction, resting-state fMRI, and multiple task fMRI paradigms. Siemens bias-field-corrected reconstructions (NORM) were stored with the BIDS `rec-norm` entity. Scanner-derived diffusion maps (ADC, FA, ColFA), localizer sequences, and IR-EPI TI series were excluded from conversion. Key HeuDiConv flags included `--converter dcm2niix`, `--bids notop` (to suppress top-level BIDS metadata regeneration on re-runs), and `--grouping all`.

## Post-BIDS Fieldmap Processing

Following DICOM conversion, a custom Python post-processing script (using nibabel and NumPy) performed three operations on each session's BIDS output. First, b=0 volumes (b < 100 s/mm²) were extracted from the posterior–anterior (PA) phase-encoded DWI acquisition, and their mean was computed to produce a three-dimensional b0 fieldmap EPI image, which was written to the `fmap/` directory with the BIDS `acq-dwi` entity alongside a copy of the original JSON sidecar. Second, `IntendedFor` fields were added to all fieldmap JSON sidecars to establish the BIDS-required linkage between field maps and their target acquisitions: DWI field maps (`acq-dwi`) were linked to anterior–posterior DWI files, and functional field maps (`acq-func`) were linked to BOLD runs. Third, spurious `.bvec` and `.bval` files in the `fmap/` directory — artifacts of the DWI-derived fieldmap creation — were hidden by prepending a dot to their filenames to prevent interference with downstream BIDS validators and processing tools.

## Anatomical Image Defacing

To protect participant privacy, all T1-weighted and T2-weighted anatomical images were defaced using `fsl_deface` from the FMRIB Software Library (FSL; Jenkinson et al., 2012). Defaced copies were written alongside the original images using the BIDS `acq-defaced` entity (e.g., `sub-XXXX_ses-YY_acq-defaced_T1w.nii.gz`), preserving the originals for processing steps that require intact facial geometry. JSON sidecars were copied alongside each defaced image.

## Diffusion MRI Preprocessing — QSIPrep

Diffusion MRI data were preprocessed using QSIPrep 1.1.1 (Cieslak et al., 2021), executed via Apptainer. Preprocessing was performed per session and included susceptibility distortion correction, eddy current correction, motion correction, and anatomical coregistration. Preprocessed diffusion data were resampled to an isotropic voxel size of 1.6 mm. The MNI152NLin2009cAsym template was used as the anatomical reference space, with a sessionwise anatomical reference strategy. A BIDS filter file restricted processing to anterior–posterior phase-encoded DWI acquisitions, with `rec-norm` T1-weighted images preferred as the anatomical input.

## Cortical Surface Reconstruction — FreeSurfer

Cortical surface reconstruction was performed using FreeSurfer 8.1.0 (Fischl, 2012), executed via Apptainer. Unlike all other pipeline stages, FreeSurfer processing was scoped at the subject level rather than the session level, with a single job processing all available sessions for a given subject.

The pipeline automatically selected between two processing strategies based on the number of available sessions. For subjects with a single session, a standard cross-sectional reconstruction was performed using `recon-all -all`. For subjects with two or more sessions, the full three-step longitudinal pipeline was executed (Reuter et al., 2012): (1) an independent cross-sectional `recon-all` for each session, (2) construction of an unbiased within-subject template via `recon-all -base` using all cross-sectional outputs as timepoints, and (3) longitudinal refinement of each session's reconstruction initialized from the template via `recon-all -long`. Already-completed steps (identified by the presence of `scripts/recon-all.done`) were automatically skipped, allowing failed jobs to resume without reprocessing.

Anatomical image selection followed a two-step filtering procedure applied per session: (1) images containing "defaced" in the filename were excluded, and (2) `rec-norm` (bias-field corrected) variants were preferred when available. When a T2-weighted image was available for a session, it was included via the `-T2` and `-T2pial` flags to improve pial surface placement.

## Diffusion MRI Reconstruction and Tractography — QSIRecon

Diffusion reconstruction and tractography were performed using QSIRecon 1.2.0 (Cieslak et al., 2021), executed via Apptainer. QSIRecon operated on the preprocessed QSIPrep outputs and required FreeSurfer cortical reconstructions for anatomically constrained tractography. Processing was governed by a custom workflow specification file that defined the full reconstruction pipeline.

### Microstructural Model Fitting

Four diffusion microstructure models were fitted in native diffusion space. Diffusion kurtosis imaging (DKI; Jensen et al., 2005) was computed using DIPY (Garyfallidis et al., 2014), including white matter tract integrity (WMTI) metrics. Neurite orientation dispersion and density imaging (NODDI; Zhang et al., 2012) was fitted using the AMICO framework (Daducci et al., 2015) with in vivo tissue parameters (d_Iso = 0.003 mm²/s, d_Par = 0.0017 mm²/s). Mean apparent propagator MRI (MAP-MRI; Ozarslan et al., 2013) was computed using DIPY with radial order 6, Laplacian regularization (weight 0.2), and a b-value threshold of 2000 s/mm². Generalized q-sampling imaging (GQI; Yeh et al., 2010) was fitted and scalar maps were exported using DSI Studio.

### Template Registration

All native-space scalar maps from the microstructural models were warped to the MNI152NLin2009cAsym template space using linear interpolation.

### Fiber Orientation Estimation and Tractography

Fiber orientation distributions were estimated using multi-shell multi-tissue constrained spherical deconvolution (MSMT-CSD; Jeurissen et al., 2014) as implemented in MRTrix3 (Tournier et al., 2019). Maximum spherical harmonic order was set to 8 for all three tissue compartments (white matter, grey matter, and cerebrospinal fluid), followed by multi-tissue intensity normalization. Tissue-specific response functions were precomputed as group-average medians from a balanced sample of 1,426 subjects.

Five-tissue-type (5TT) segmentation images were generated from FreeSurfer outputs using the hybrid surface–volume segmentation (HSVS) algorithm. Two whole-brain tractography approaches were employed. Probabilistic tractography was performed using the iFOD2 algorithm (Tournier et al., 2010) with anatomical constraints (ACT; Smith et al., 2012), generating 1,000,000 streamlines with lengths between 30 and 250 mm, with backtracking enabled. Deterministic tractography was performed using the SD_Stream algorithm with the same streamline count and length constraints. Both tractograms were filtered using SIFT2 (Smith et al., 2015) to improve the biological plausibility of streamline weights.

### Structural Connectivity

Structural connectivity matrices were constructed from both probabilistic and deterministic tractograms using two brain parcellation atlases: the 4S156Parcels atlas and the Schaefer 100-parcel 7-network parcellation combined with the Tian subcortical atlas at scale S1 (Schaefer et al., 2018; Tian et al., 2020). For each atlas and tractography method, four connectivity measures were computed: (1) SIFT2-weighted streamline count with inverse-node-volume scaling, (2) mean streamline length, (3) raw streamline count, and (4) SIFT2-weighted streamline count. All matrices were symmetrized, used a search radius of 2 mm for streamline–parcel assignment, and retained self-connections (non-zero diagonal).

### White Matter Bundle Tractometry

Along-tract profiling was performed on the probabilistic (iFOD2) tractogram using pyAFQ (Kruper et al., 2021). The AFQ segmentation algorithm was applied to identify approximately 24 major white matter bundles. Tissue-property profiles were sampled at 100 equidistant nodes along each bundle using Gaussian weighting. Fractional anisotropy (FA) and mean diffusivity (MD) from diffusion tensor imaging were extracted as scalar measures. Key segmentation parameters included a distance threshold of 3 mm, distance-to-atlas threshold of 4 mm, minimum of 20 streamlines per bundle, and streamline length range of 50–250 mm.

## References

Cieslak, M., Cook, P. A., He, X., Yeh, F.-C., Dhollander, T., Adebimpe, A., ... & Satterthwaite, T. D. (2021). QSIPrep: An integrative platform for preprocessing and reconstructing diffusion MRI data. *Nature Methods*, 18(7), 775–778.

Daducci, A., Canales-Rodríguez, E. J., Zhang, H., Dyrby, T. B., Alexander, D. C., & Thiran, J.-P. (2015). Accelerated Microstructure Imaging via Convex Optimization (AMICO) from diffusion MRI data. *NeuroImage*, 105, 32–44.

Fischl, B. (2012). FreeSurfer. *NeuroImage*, 62(2), 774–781.

Garyfallidis, E., Brett, M., Amirbekian, B., Rokem, A., van der Walt, S., Descoteaux, M., & Nimmo-Smith, I. (2014). Dipy, a library for the analysis of diffusion MRI data. *Frontiers in Neuroinformatics*, 8, 8.

Gorgolewski, K. J., Auer, T., Calhoun, V. D., Craddock, R. C., Das, S., Duff, E. P., ... & Poldrack, R. A. (2016). The brain imaging data structure, a format for organizing and describing outputs of neuroimaging experiments. *Scientific Data*, 3, 160044.

Halchenko, Y. O., Goncalves, M., Ghosh, S., Velasco, P., Visconti di Oleggio Castello, M., Salo, T., ... & Hanke, M. (2024). HeuDiConv — flexible DICOM conversion into structured directory layouts. *Journal of Open Source Software*, 9(99), 5839.

Jenkinson, M., Beckmann, C. F., Behrens, T. E. J., Woolrich, M. W., & Smith, S. M. (2012). FSL. *NeuroImage*, 62(2), 782–790.

Jensen, J. H., Helpern, J. A., Ramani, A., Lu, H., & Kaczynski, K. (2005). Diffusional kurtosis imaging: The quantification of non-Gaussian water diffusion by means of magnetic resonance imaging. *Magnetic Resonance in Medicine*, 53(6), 1432–1440.

Jeurissen, B., Tournier, J.-D., Dhollander, T., Connelly, A., & Sijbers, J. (2014). Multi-tissue constrained spherical deconvolution for improved analysis of multi-shell diffusion MRI data. *NeuroImage*, 103, 411–426.

Kruper, J., Yeatman, J. D., Richie-Halford, A., Bloom, D., Grotheer, M., Caffarra, S., ... & Rokem, A. (2021). Evaluating the reliability of human brain white matter tractometry. *Aperture Neuro*, 1(1).

Li, X., Morgan, P. S., Ashburner, J., Smith, J., & Rorden, C. (2016). The first step for neuroimaging data analysis: DICOM to NIfTI conversion. *Journal of Neuroscience Methods*, 264, 47–56.

Ozarslan, E., Koay, C. G., Shepherd, T. M., Komlosh, M. E., İrfanoğlu, M. O., Pierpaoli, C., & Basser, P. J. (2013). Mean apparent propagator (MAP) MRI: A novel diffusion imaging method for mapping tissue microstructure. *NeuroImage*, 78, 16–32.

Reuter, M., Schmansky, N. J., Rosas, H. D., & Fischl, B. (2012). Within-subject template estimation for unbiased longitudinal image analysis. *NeuroImage*, 61(4), 1402–1418.

Schaefer, A., Kong, R., Gordon, E. M., Laumann, T. O., Zuo, X.-N., Holmes, A. J., ... & Yeo, B. T. T. (2018). Local-global parcellation of the human cerebral cortex from intrinsic functional connectivity MRI. *Cerebral Cortex*, 28(9), 3095–3114.

Smith, R. E., Tournier, J.-D., Calamante, F., & Connelly, A. (2012). Anatomically-constrained tractography: Improved diffusion MRI streamlines tractography through effective use of anatomical information. *NeuroImage*, 62(3), 1924–1938.

Smith, R. E., Tournier, J.-D., Calamante, F., & Connelly, A. (2015). SIFT2: Enabling dense quantitative assessment of brain white matter connectivity using streamlines tractography. *NeuroImage*, 119, 338–351.

Tian, Y., Margulies, D. S., Breakspear, M., & Zalesky, A. (2020). Topographic organization of the human subcortex unveiled with functional connectivity gradients. *Nature Neuroscience*, 23(11), 1421–1432.

Tournier, J.-D., Calamante, F., & Connelly, A. (2010). Improved probabilistic streamlines tractography by 2nd order integration over fibre orientation distributions. In *Proceedings of the International Society for Magnetic Resonance in Medicine* (Vol. 18, p. 1670).

Tournier, J.-D., Smith, R., Raffelt, D., Tabbara, R., Dhollander, T., Pietsch, M., ... & Connelly, A. (2019). MRtrix3: A fast, flexible and open software framework for medical image processing and visualisation. *NeuroImage*, 202, 116137.

Yeh, F.-C., Wedeen, V. J., & Tseng, W.-Y. I. (2010). Generalized q-sampling imaging. *IEEE Transactions on Medical Imaging*, 29(9), 1626–1635.

Zhang, H., Schneider, T., Wheeler-Kingshott, C. A., & Alexander, D. C. (2012). NODDI: Practical in vivo neurite orientation dispersion and density imaging of the human brain. *NeuroImage*, 61(4), 1000–1016.
